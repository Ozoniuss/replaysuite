package replaysuite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
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

	tclient client.Client

	activityMock mock.Mock
	workflowMock mock.Mock

	// Note that each environment assumes that one workflow and one
	// activity type are associated with one function.
	activityStubFnRef map[string]any
	workflowStubFnRef map[string]any

	dumpHistory dumpHistoryFn

	// only populated after execution on dev server completes.
	workflowHistoryResult *historypb.History
}

type dumpHistoryFn func(wfType, wfId string, hist *historypb.History) error

func (e *TestWorkflowEnvironment) ExecuteWorkflow(workflowFn any, args ...any) {
	e.TestWorkflowEnvironment.ExecuteWorkflow(workflowFn, args...)

	if err := e.mirrorOnDevServer(workflowFn, args); err != nil {
		panic(fmt.Sprintf("[replaysuite] dev-server mirror failed: %v", err))
	}
}
func (e *TestWorkflowEnvironment) OnActivity(activityFn any, args ...any) *MockCallWrapper {

	name, _ := getFunctionName(activityFn)

	if _, ok := e.activityStubFnRef[name]; !ok {
		e.activityStubFnRef[name] = activityFn
	}

	return &MockCallWrapper{
		call:  e.activityMock.On(name, args...),
		inner: e.TestWorkflowEnvironment.OnActivity(activityFn, args...),
	}
}

func (e *TestWorkflowEnvironment) OnWorkflow(workflowFn any, args ...any) *MockCallWrapper {

	name, _ := getFunctionName(workflowFn)

	if _, ok := e.workflowStubFnRef[name]; !ok {
		e.workflowStubFnRef[name] = workflowFn
	}

	return &MockCallWrapper{
		call:  e.workflowMock.On(name, args...),
		inner: e.TestWorkflowEnvironment.OnWorkflow(workflowFn, args...),
	}
}

func (e *TestWorkflowEnvironment) mirrorOnDevServer(workflowFn any, args []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a new worker for each executed workflow. If this becomes expensive,
	// it may be a good idea to use shared workers.
	taskQueue := "replaysuite-" + uuid.NewString()
	w := worker.New(e.tclient, taskQueue, worker.Options{
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

	run, err := e.tclient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        taskQueue,
		TaskQueue: taskQueue,
	}, workflowFn, args...)
	if err != nil {
		return fmt.Errorf("execute workflow: %w", err)
	}
	// TODO: maybe the workflow errored somehow on dev server (e.g. missing
	// activity policy). Figure out what to do there.
	if err := run.Get(ctx, nil); err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() == internalErrorType {
			return fmt.Errorf("workflow failed due to replaysuite error: %w", err)
		}
	}

	hist := &historypb.History{}
	iter := e.tclient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return fmt.Errorf("read history: %w", err)
		}
		hist.Events = append(hist.Events, ev)
	}

	wfType, _ := getFunctionName(workflowFn)
	err = e.dumpHistory(wfType, taskQueue, hist)
	if err != nil {
		return fmt.Errorf("dump history: %w", err)
	}
	e.workflowHistoryResult = hist

	return nil
}

func (env *TestWorkflowEnvironment) dynamicActivityHandler(ctx context.Context, payloads converter.EncodedValues) (any, error) {
	info := activity.GetInfo(ctx)
	wfID := info.WorkflowExecution.ID
	actName := info.ActivityType.Name

	stubFn, ok := env.activityStubFnRef[actName]
	if !ok {
		return nil, internalError(fmt.Errorf("replaysuite: no stub for activity %s in workflow %s", actName, wfID))
	}

	callArgs, err := decodePayloadsViaFnType(ctx, payloads, stubFn)
	if err != nil {
		return nil, internalError(fmt.Errorf("decode payloads for %s: %w", actName, err))
	}

	rets, err := methodCalled(&env.activityMock, actName, callArgs)
	if err != nil {
		return nil, internalError(fmt.Errorf("replaysuite: activity %s: %w", actName, err))
	}
	return extractReturn(rets)
}

func (env *TestWorkflowEnvironment) dynamicWorkflowHandler(ctx workflow.Context, payloads converter.EncodedValues) (any, error) {
	info := workflow.GetInfo(ctx)
	wfType := info.WorkflowType.Name

	// note that we return internalError so that it's clear if the dev server
	// failed unexpectedly.
	parent := info.ParentWorkflowExecution
	if parent == nil {
		return nil, internalError(fmt.Errorf("replaysuite: workflow %s has no parent; only mocked child workflows are served by the dynamic handler", wfType))
	}

	stubFn, ok := env.workflowStubFnRef[wfType]
	if !ok {
		return nil, internalError(fmt.Errorf("replaysuite: no OnWorkflow stub for child workflow %s in workflow %s", wfType, parent.ID))
	}

	callArgs, err := decodePayloadsViaFnType(ctx, payloads, stubFn)
	if err != nil {
		return nil, internalError(fmt.Errorf("decode payloads for child workflow %s: %w", wfType, err))
	}

	rets, err := methodCalled(&env.workflowMock, wfType, callArgs)
	if err != nil {
		return nil, internalError(fmt.Errorf("replaysuite: child workflow %s: %w", wfType, err))
	}
	return extractReturn(rets)
}

const internalErrorType = "ReplaySuiteInternalError"

func internalError(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), internalErrorType, err)
}

// methodCalled reports a call to m. Without a TestingT, testify panics on
// unexpected or exhausted calls, so the panic is converted into an error.
func methodCalled(m *mock.Mock, name string, args []any) (rets mock.Arguments, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return m.MethodCalled(name, args...), nil
}

// decodePayloadsViaFnType decodes payloads into the argument types of fn. If
// fn takes a context as its first argument, ctx is prepended to the result.
func decodePayloadsViaFnType(ctx any, payloads converter.EncodedValues, fn any) ([]any, error) {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("not a function")
	}
	ctxType := reflect.TypeFor[context.Context]()
	wfCtxType := reflect.TypeFor[workflow.Context]()

	startIdx := 0
	if fnType.NumIn() > 0 {
		if first := fnType.In(0); first.Implements(ctxType) || first.Implements(wfCtxType) {
			startIdx = 1
		}
	}
	argCount := fnType.NumIn() - startIdx
	ptrs := make([]any, argCount)
	values := make([]reflect.Value, argCount)
	for i := range argCount {
		t := fnType.In(i + startIdx)
		v := reflect.New(t)
		ptrs[i] = v.Interface()
		values[i] = v
	}
	if argCount > 0 {
		if err := payloads.Get(ptrs...); err != nil {
			return nil, err
		}
	}
	out := make([]any, 0, startIdx+argCount)
	if startIdx == 1 {
		out = append(out, ctx)
	}
	for _, v := range values {
		out = append(out, v.Elem().Interface())
	}
	return out, nil
}

func extractReturn(rets []any) (any, error) {
	switch len(rets) {
	case 0:
		return nil, nil
	case 1:
		if rets[0] == nil {
			return nil, nil
		}
		if e, ok := rets[0].(error); ok {
			return nil, e
		}
		return rets[0], nil
	default:
		var err error
		if rets[1] != nil {
			if e, ok := rets[1].(error); ok {
				err = e
			}
		}
		return rets[0], err
	}
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
