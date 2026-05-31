package replaysuite

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Env wraps *testsuite.TestWorkflowEnvironment. It is meant to be used
// as a drop-in replacement for *testsuite.TestWorkflowEnvironment such
// that the previously-written tests keep working as-is, on top of which
// event histories are generated for each ExecuteWorkflow call. These
// event histories can later be used for replay testing.
//
// In order to produce event histories, the replay test environment
// spins up an in-process temporal cluster and a shared worker and
// registers all workflows and activities on that shared worker. This
// is sufficient for replay testing given that the actual worker
// running the activity or the activity options don't actually matter
// for replay testing.
type Env struct {
	*testsuite.TestWorkflowEnvironment

	suite    *WorkflowTestSuite
	envIdx   int64
	runCount int64

	// Activity stubs recorded by OnActivity. The shared worker's dynamic
	// activity handler routes each activity call back to this table via
	// the workflow ID (see Suite.dynamicActivityHandler).
	stubs     map[string][]stubCall
	stubFnRef map[string]any

	// Child-workflow stubs recorded by OnWorkflow. The shared worker's
	// dynamic workflow handler routes each child-workflow call back to this
	// table via the parent workflow ID (see Suite.dynamicWorkflowHandler).
	workflowStubs     map[string][]stubCall
	workflowStubFnRef map[string]any
}

// stubCall records one OnActivity(...).Return(...) registration. The first
// arg passed to OnActivity is treated as the context placeholder (e.g.
// mock.Anything) and stripped before matching against runtime args.
type stubCall struct {
	expectedArgs []any
	returns      []any
}

// MockCallWrapper overwrites the one from TestWorkflowEnvironment. It
// keeps a reference to the inner mock call wrapper so that it can
// still run the existing test suite.
//
// TODO: Implement all methods from the original MockCallWrapper.
type MockCallWrapper struct {
	env     *Env
	name    string
	stubIdx int
	// isWorkflow distinguishes a wrapper created by OnWorkflow from one
	// created by OnActivity, so Return writes the recorded result into the
	// matching stub table.
	isWorkflow bool
	inner      *testsuite.MockCallWrapper
}

func (m *MockCallWrapper) Return(returnArguments ...any) *MockCallWrapper {
	if m.isWorkflow {
		m.env.workflowStubs[m.name][m.stubIdx].returns = returnArguments
	} else {
		m.env.stubs[m.name][m.stubIdx].returns = returnArguments
	}
	m.inner.Return(returnArguments...)
	return m
}

func (m *MockCallWrapper) Once() *MockCallWrapper {
	m.inner.Once()
	return m
}

func (e *Env) OnActivity(activityFn any, args ...any) *MockCallWrapper {

	// TODO: do we need to handle method registration?
	name, _ := activityFunctionName(activityFn)

	// First arg is the context placeholder (mock.Anything by convention).
	// Strip it for our matcher; the dev-server stub never receives it as
	// a user-visible argument.
	matchArgs := []any{}
	if len(args) > 1 {
		matchArgs = args[1:]
	}

	e.stubs[name] = append(e.stubs[name], stubCall{expectedArgs: matchArgs})
	if _, ok := e.stubFnRef[name]; !ok {
		e.stubFnRef[name] = activityFn
	}

	return &MockCallWrapper{
		env:     e,
		name:    name,
		stubIdx: len(e.stubs[name]) - 1,
		inner:   e.TestWorkflowEnvironment.OnActivity(activityFn, args...),
	}
}

// OnWorkflow is the child-workflow counterpart of OnActivity. It records a
// stub so the shared worker's dynamic workflow handler can serve the child
// workflow with the recorded result, and forwards to the inner env so the
// unit test's mock behaves exactly as before.
//
// Like OnActivity, the first arg is the context placeholder (mock.Anything
// by convention) — here it stands in for the child's workflow.Context — and
// is stripped before matching against runtime args.
func (e *Env) OnWorkflow(workflowFn any, args ...any) *MockCallWrapper {
	name, _ := activityFunctionName(workflowFn)

	matchArgs := []any{}
	if len(args) > 1 {
		matchArgs = args[1:]
	}

	e.workflowStubs[name] = append(e.workflowStubs[name], stubCall{expectedArgs: matchArgs})
	if _, ok := e.workflowStubFnRef[name]; !ok {
		e.workflowStubFnRef[name] = workflowFn
	}

	return &MockCallWrapper{
		env:        e,
		name:       name,
		stubIdx:    len(e.workflowStubs[name]) - 1,
		isWorkflow: true,
		inner:      e.TestWorkflowEnvironment.OnWorkflow(workflowFn, args...),
	}
}

// RegisterWorkflow forwards to the inner env only. It is required for any
// workflow the user invokes by name (child workflows, continue-as-new
// targets) so the inner TestWorkflowEnvironment can dispatch it.
//
// On the shared dev-server mirror, child workflows are served by the dynamic
// workflow handler in suite.go, which routes back to this env's OnWorkflow
// stubs. Registering the real workflow implementation on the shared worker
// would shadow that handler (explicit registration takes precedence over the
// dynamic one), so we deliberately don't — child workflows reached on the
// mirror must be mocked via OnWorkflow, exactly as activities must be mocked
// via OnActivity. This mirrors how RegisterActivity is handled.
func (e *Env) RegisterWorkflow(w any) {
	e.TestWorkflowEnvironment.RegisterWorkflow(w)
}

func (e *Env) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	e.TestWorkflowEnvironment.RegisterWorkflowWithOptions(w, options)
}

// RegisterActivity / RegisterActivityWithOptions only forward to the inner
// env. On the shared mirror, all activities are served by the dynamic
// handler in suite.go, which routes back to this env's OnActivity stubs.
// Registering real activity implementations on the shared worker would
// fight with the dynamic handler, so we deliberately don't.
func (e *Env) RegisterActivity(a any) {
	e.TestWorkflowEnvironment.RegisterActivity(a)
}

func (e *Env) RegisterActivityWithOptions(a any, options activity.RegisterOptions) {
	e.TestWorkflowEnvironment.RegisterActivityWithOptions(a, options)
}

// ExecuteWorkflow runs the workflow against the inner test environment
// (this is what determines pass/fail of the unit test) and then mirrors
// the same run against the suite's shared dev-server worker so a real
// history is recorded.
func (e *Env) ExecuteWorkflow(workflowFn any, args ...any) {
	e.TestWorkflowEnvironment.ExecuteWorkflow(workflowFn, args...)

	if err := e.mirrorOnDevServer(workflowFn, args); err != nil {
		panic(fmt.Sprintf("[replaysuite] dev-server mirror failed: %v", err))
	}
}

func (e *Env) mirrorOnDevServer(workflowFn any, args []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Register the workflow on the shared mirror worker before starting it.
	// A real worker.Worker does not auto-register workflows the way the inner
	// TestWorkflowEnvironment does, so without this the dev-server task fails
	// with "unable to find workflow type" until the mirror times out.
	if _, err := e.registerWorkflowOnShared(workflowFn, workflow.RegisterOptions{
		DisableAlreadyRegisteredCheck: true,
	}); err != nil {
		return err
	}
	if err := e.suite.startWorkerOnce(); err != nil {
		return err
	}

	runIdx := atomic.AddInt64(&e.runCount, 1)
	wfID := fmt.Sprintf("replaysuite-env%d-run%d", e.envIdx, runIdx)

	e.suite.envsByWFID.Store(wfID, e)
	defer e.suite.envsByWFID.Delete(wfID)

	run, err := e.suite.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: sharedTaskQueue,
	}, workflowFn, args...)
	if err != nil {
		return fmt.Errorf("execute workflow: %w", err)
	}
	// Discard result/error — the inner env is the source of truth for the
	// test; the mirror only exists to produce a history.
	_ = run.Get(ctx, nil)
	return nil
}

// registerWorkflowOnShared registers a workflow function on the suite's
// shared worker, deduping by function pointer so the same workflow isn't
// re-registered on every test.
func (e *Env) registerWorkflowOnShared(w any, opts workflow.RegisterOptions) (string, error) {
	workflowType := workflowTypeName(w, opts)
	if _, ok := w.(string); ok {
		return workflowType, nil
	}

	if err := e.suite.ensureWorkerCreated(); err != nil {
		return workflowType, fmt.Errorf("create shared replay worker: %w", err)
	}
	if _, loaded := e.suite.workflowSet.LoadOrStore(workflowType, struct{}{}); loaded {
		return workflowType, nil
	}
	e.suite.worker.RegisterWorkflowWithOptions(w, opts)
	return workflowType, nil
}

func workflowTypeName(w any, opts workflow.RegisterOptions) string {
	if opts.Name != "" {
		return opts.Name
	}
	name, _ := activityFunctionName(w)
	return name
}

// activityFunctionName is a straight copy from what Go SDK does.
// https://github.com/temporalio/sdk-go/blob/35367242edb9e92be90825cd7653ab0046a660d1/internal/internal_worker.go#L2565
func activityFunctionName(i any) (name string, isMethod bool) {
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
