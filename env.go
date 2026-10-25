package replaysuite

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestWorkflowEnvironment is a wrapper around *testsuite.TestWorkflowEnvironment
// that simulates real workflow execitions on a temporal cluster. Existing tests
// will continue working as expected.
//
// Note that in order to use the replay suite, you must use this instead of the
// original *testsuite.TestWorkflowEnvironment.
type TestWorkflowEnvironment struct {
	*testsuite.TestWorkflowEnvironment

	suite *WorkflowTestSuite

	activityStubs     map[string][]stubCall
	activityStubFnRef map[string]any

	workflowStubs     map[string][]stubCall
	workflowStubFnRef map[string]any
}

func (e *TestWorkflowEnvironment) ExecuteWorkflow(workflowFn any, args ...any) {
	e.TestWorkflowEnvironment.ExecuteWorkflow(workflowFn, args...)

	if err := e.mirrorOnDevServer(workflowFn, args); err != nil {
		panic(fmt.Sprintf("[replaysuite] dev-server mirror failed: %v", err))
	}
}
func (e *TestWorkflowEnvironment) OnActivity(activityFn any, args ...any) *MockCallWrapper {

	name, _ := getFunctionName(activityFn)

	matchArgs := []any{}
	if len(args) > 1 {
		matchArgs = args[1:]
	}

	// TODO: this can be called multiple times for the same activity or
	// workflow, e.g. when you run it twice. Need to cover that case.
	e.activityStubs[name] = append(e.activityStubs[name], stubCall{expectedArgs: matchArgs})
	if _, ok := e.activityStubFnRef[name]; !ok {
		e.activityStubFnRef[name] = activityFn
	}

	return &MockCallWrapper{
		env:     e,
		name:    name,
		stubIdx: len(e.activityStubs[name]) - 1,
		call:    onActivityCall,
		inner:   e.TestWorkflowEnvironment.OnActivity(activityFn, args...),
	}

}

func (e *TestWorkflowEnvironment) OnWorkflow(workflowFn any, args ...any) *MockCallWrapper {

	name, _ := getFunctionName(workflowFn)

	matchArgs := []any{}
	if len(args) > 1 {
		matchArgs = args[1:]
	}

	e.workflowStubs[name] = append(e.workflowStubs[name], stubCall{expectedArgs: matchArgs})
	if _, ok := e.workflowStubFnRef[name]; !ok {
		e.workflowStubFnRef[name] = workflowFn
	}

	return &MockCallWrapper{
		env:     e,
		name:    name,
		stubIdx: len(e.workflowStubs[name]) - 1,
		call:    onWorkflowCall,
		inner:   e.TestWorkflowEnvironment.OnWorkflow(workflowFn, args...),
	}
}

func (e *TestWorkflowEnvironment) mirrorOnDevServer(workflowFn any, args []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a new worker for each executed workflow. If this becomes expensive,
	// it may be a good idea to use shared workers.
	taskQueue := "replaysuite-" + uuid.NewString()
	w := worker.New(e.suite.client, taskQueue, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{&fastReplayInterceptor{taskQueue: taskQueue}},
	})
	if _, ok := workflowFn.(string); !ok {
		w.RegisterWorkflow(workflowFn)
	}

	// run mocked code
	// todo: handle actual activities
	w.RegisterDynamicActivity(e.dynamicActivityHandler, activity.DynamicRegisterOptions{})
	w.RegisterDynamicWorkflow(e.dynamicWorkflowHandler, workflow.DynamicRegisterOptions{})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	run, err := e.suite.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        taskQueue,
		TaskQueue: taskQueue,
	}, workflowFn, args...)
	if err != nil {
		return fmt.Errorf("execute workflow: %w", err)
	}
	// TODO: maybe the workflow errored somehow on dev server (e.g. missing
	// activity policy). Figure out what to do there.
	_ = run.Get(ctx, nil)
	return nil
}

type stubCall struct {
	expectedArgs []any
	returns      []any
}

type overriddenCall string

const (
	onActivityCall overriddenCall = "OnActivity"
	onWorkflowCall overriddenCall = "OnWorkflow"
)

// TODO: Implement all methods from the original MockCallWrapper.
type MockCallWrapper struct {
	env     *TestWorkflowEnvironment
	name    string
	stubIdx int
	call    overriddenCall
	inner   *testsuite.MockCallWrapper
}

func (m *MockCallWrapper) Return(returnArguments ...any) *MockCallWrapper {
	switch m.call {
	case onActivityCall:
		m.env.activityStubs[m.name][m.stubIdx].returns = returnArguments
	case onWorkflowCall:
		m.env.workflowStubs[m.name][m.stubIdx].returns = returnArguments
	}
	m.inner.Return(returnArguments...)
	return m
}

func (m *MockCallWrapper) Once() *MockCallWrapper {
	m.inner.Once()
	return m
}

// getFunctionName is a straight copy from what Go SDK does.
// https://github.com/temporalio/sdk-go/blob/35367242edb9e92be90825cd7653ab0046a660d1/internal/internal_worker.go#L2565
func getFunctionName(i any) (name string, isMethod bool) {
	if fullName, ok := i.(string); ok {
		return fullName, false
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	// Full function name that has a struct pointer receiver has the following format
	// <prefix>.(*<type>).<function>
	isMethod = strings.ContainsAny(fullName, "*")
	elements := strings.Split(fullName, ".")
	shortName := elements[len(elements)-1]
	// This allows to call activities by method pointer
	// Compiler adds -fm suffix to a function name which has a receiver
	// Note that this works even if struct pointer used to get the function is nil
	// It is possible because nil receivers are allowed.
	// For example:
	// var a *Activities
	// ExecuteActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm"), isMethod
}
