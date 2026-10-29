package replaysuite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
	_ "unsafe"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	tclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	REAL_EXECUTION_TASK_QUEUE = "replaysuite"
)

var testclient tclient.Client
var testworker worker.Worker

func TestMain(m *testing.M) {

	err := os.RemoveAll(".histories")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to clean up old histories: %s", err.Error())
		os.Exit(1)
	}

	useRealServer := os.Getenv("REPLAYSUITE_TEST_WITH_REAL_SERVER")
	var srv *testsuite.DevServer

	if useRealServer == "true" {
		// allow developing with a real server to compare histories
		testclient, err = tclient.Dial(tclient.Options{
			HostPort: "localhost:7233",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to existing dev server %s", err.Error())
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "using real server")

		// TODO: a single shared worker might have registration conflicts e.g.
		// when using anonymous functions, which get registered via the name
		// inferred at runtime.
		testworker = worker.New(testclient, REAL_EXECUTION_TASK_QUEUE, worker.Options{})
		testworker.Start()

	} else {
		srv, err = testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{
			ExtraArgs: []string{
				"--dynamic-config-value", `history.timerProcessorMaxTimeShift="1ms"`,
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start dev server: %s", err.Error())
			os.Exit(1)
		}
		testclient = srv.Client()
		fmt.Fprintf(os.Stdout, "using test-managed server")
	}
	code := m.Run()

	if srv != nil {
		_ = srv.Stop()
	}
	if testworker != nil {
		testworker.Stop()
	}
	testclient.Close()

	os.Exit(code)
}

func newTestEnv(s testsuite.WorkflowTestSuite) TestWorkflowEnvironment {
	return TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		actSignRef:              map[string]any{},
		wfSignRef:               map[string]any{},
		actMocked:               map[string]struct{}{},
	}
}

// === Test for a simple workflow with one activity

func ActivitySimpleFlow(ctx context.Context) error {
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}
func WorkflowSimpleFlow(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, ActivitySimpleFlow).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_SimpleFlow(t *testing.T) {

	testworker.RegisterWorkflow(WorkflowSimpleFlow)
	testworker.RegisterActivity(ActivitySimpleFlow)
	// testworker.RegisterActivity(ActivitySimpleFlow)
	_, err := testclient.ExecuteWorkflow(t.Context(), tclient.StartWorkflowOptions{
		ID:        "Test Simple Flow",
		TaskQueue: REAL_EXECUTION_TASK_QUEUE,
	}, WorkflowSimpleFlow)
	require.Nil(t, err)

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(ActivitySimpleFlow, mock.Anything).Return(nil)

	env.ExecuteWorkflow(WorkflowSimpleFlow)
	require.True(t, env.IsWorkflowCompleted())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	// This is the name inferred for anonymous functions.
	require.Equal(t, "WorkflowSimpleFlow", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivitySimpleFlow", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

// === Test that activity arguments coming from mocks reach the dev server (it
// doesn't matter for history but it matters for workflow execution e.g.
// branching logic)

func ActivityWithArgs(ctx context.Context, name string, count int) (string, error) {
	return fmt.Sprintf("real:%s:%d", name, count), nil
}
func WorkflowWithArgs(ctx workflow.Context, name string, count int) (string, error) {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	var result string
	if err := workflow.ExecuteActivity(ctx, ActivityWithArgs, name, count).Get(ctx, &result); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return "", err
	}
	return result, nil
}

func TestEnvironment_WithArgs(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(ActivityWithArgs, mock.Anything, "alice", 3).Return("mocked:alice:3", nil)

	env.ExecuteWorkflow(WorkflowWithArgs, "alice", 3)
	require.True(t, env.IsWorkflowCompleted())

	var envResult string
	require.NoError(t, env.GetWorkflowResult(&envResult))
	require.Equal(t, "mocked:alice:3", envResult)

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowWithArgs", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityWithArgs", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())

	dc := converter.GetDefaultDataConverter()

	var wfName string
	var wfCount int
	require.NoError(t, dc.FromPayloads(hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetInput(), &wfName, &wfCount))
	require.Equal(t, "alice", wfName)
	require.Equal(t, 3, wfCount)

	var actName string
	var actCount int
	require.NoError(t, dc.FromPayloads(hist.Events[4].GetActivityTaskScheduledEventAttributes().GetInput(), &actName, &actCount))
	require.Equal(t, "alice", actName)
	require.Equal(t, 3, actCount)

	var actResult string
	require.NoError(t, dc.FromPayloads(hist.Events[6].GetActivityTaskCompletedEventAttributes().GetResult(), &actResult))
	require.Equal(t, "mocked:alice:3", actResult)

	var wfResult string
	require.NoError(t, dc.FromPayloads(hist.Events[10].GetWorkflowExecutionCompletedEventAttributes().GetResult(), &wfResult))
	require.Equal(t, "mocked:alice:3", wfResult)
}

// === Test that activity arguments on reach the server when activties are
// supposed to run as expected.

func TestEnvironment_WithArgsNotMocked(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.RegisterActivity(ActivityWithArgs)

	env.ExecuteWorkflow(WorkflowWithArgs, "alice", 3)
	require.True(t, env.IsWorkflowCompleted())

	var envResult string
	require.NoError(t, env.GetWorkflowResult(&envResult))
	require.Equal(t, "real:alice:3", envResult)

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowWithArgs", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityWithArgs", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())

	dc := converter.GetDefaultDataConverter()

	var wfName string
	var wfCount int
	require.NoError(t, dc.FromPayloads(hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetInput(), &wfName, &wfCount))
	require.Equal(t, "alice", wfName)
	require.Equal(t, 3, wfCount)

	var actName string
	var actCount int
	require.NoError(t, dc.FromPayloads(hist.Events[4].GetActivityTaskScheduledEventAttributes().GetInput(), &actName, &actCount))
	require.Equal(t, "alice", actName)
	require.Equal(t, 3, actCount)

	var actResult string
	require.NoError(t, dc.FromPayloads(hist.Events[6].GetActivityTaskCompletedEventAttributes().GetResult(), &actResult))
	require.Equal(t, "real:alice:3", actResult)

	var wfResult string
	require.NoError(t, dc.FromPayloads(hist.Events[10].GetWorkflowExecutionCompletedEventAttributes().GetResult(), &wfResult))
	require.Equal(t, "real:alice:3", wfResult)
}

// === Test that a mocked activity error drives the workflow logic on the dev
// server, so the second activity is never scheduled.

func ActivityBranchingFirst(ctx context.Context) error {
	return nil
}
func ActivityBranchingSecond(ctx context.Context) error {
	return nil
}
func WorkflowBranching(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, ActivityBranchingFirst).Get(ctx, nil); err != nil {
		logger.Error("First activity failed, skipping the second one.", "Error", err)
		return err
	}
	if err := workflow.ExecuteActivity(ctx, ActivityBranchingSecond).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_Branching(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(ActivityBranchingFirst, mock.Anything).Return(
		temporal.NewNonRetryableApplicationError("first failed", "BranchingError", nil),
	)
	env.OnActivity(ActivityBranchingSecond, mock.Anything).Return(nil).Never()

	env.ExecuteWorkflow(WorkflowBranching)
	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "first failed")

	// The second activity is called neither in the test environment nor on the
	// dev server.
	env.AssertActivityNotCalled(t, "ActivityBranchingSecond", mock.Anything)
	env.activityMock.AssertNotCalled(t, "ActivityBranchingSecond", mock.Anything)

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
	}, eventTypes)

	require.Equal(t, "WorkflowBranching", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityBranchingFirst", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
	require.Equal(t, "first failed", hist.Events[6].GetActivityTaskFailedEventAttributes().GetFailure().GetMessage())
}

// === Test a failed activity, plus that its retry policy is overridden and there
// aren't plenty of retries making the test slow.

func ActivityFailed(ctx context.Context) error {
	return nil
}
func WorkflowActivityFailed(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    100 * time.Second,
				MaximumAttempts:    50,
			},
		},
	)

	return workflow.ExecuteActivity(ctx, ActivityFailed).Get(ctx, nil)
}

func TestEnvironment_ActivityFailed(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(ActivityFailed, mock.Anything).Return(errors.New("activity failed"))

	env.ExecuteWorkflow(WorkflowActivityFailed)
	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "activity failed")

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
	}, eventTypes)

	retryPolicy := hist.Events[4].GetActivityTaskScheduledEventAttributes().GetRetryPolicy()
	require.Equal(t, shortRetryInterval, retryPolicy.GetInitialInterval().AsDuration())
	require.Equal(t, shortRetryInterval, retryPolicy.GetMaximumInterval().AsDuration())
	require.Equal(t, 1.0, retryPolicy.GetBackoffCoefficient())
	require.Equal(t, int32(1), retryPolicy.GetMaximumAttempts())

	require.Equal(t, int32(1), hist.Events[5].GetActivityTaskStartedEventAttributes().GetAttempt())
}

// === Test task queue interceptor override, which is crucial to making sure that
// the test suite will actually be able to execute it (since it only has one worker
// and one task queue).

func ActivityCustomTaskQueue(ctx context.Context) error {
	return nil
}
func WorkflowCustomTaskQueue(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
			TaskQueue:              "activity-task-queue",
		},
	)

	return workflow.ExecuteActivity(ctx, ActivityCustomTaskQueue).Get(ctx, nil)
}

func TestEnvironment_CustomTaskQueue(t *testing.T) {

	const envTaskQueue = "env-task-queue"

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	// explicitly overwite the task queue so we can compare
	env.taskQueue = envTaskQueue
	env.OnActivity(ActivityCustomTaskQueue, mock.Anything).Return(nil)

	env.ExecuteWorkflow(WorkflowCustomTaskQueue)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, envTaskQueue, hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetTaskQueue().GetName())
	require.Equal(t, envTaskQueue, hist.Events[4].GetActivityTaskScheduledEventAttributes().GetTaskQueue().GetName())
}

// === Test that regular registration with no mock will run the actual activity

var regularRegistrationCalls atomic.Int32

func ActivityRegularRegistration(ctx context.Context) error {
	regularRegistrationCalls.Add(1)
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}
func WorkflowRegularRegistration(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, ActivityRegularRegistration).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_RegularRegistration(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.RegisterActivity(ActivityRegularRegistration)
	env.ExecuteWorkflow(WorkflowRegularRegistration)
	require.True(t, env.IsWorkflowCompleted())

	// ExecuteWorkflow waits for the dev mirror to complete execution so this
	// call is safe.
	require.Equal(t, int32(2), regularRegistrationCalls.Load())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowRegularRegistration", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityRegularRegistration", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

// === Test that regular registration with mock will run the mock instead.

var mockOverwritesRegularCalls atomic.Int32

func ActivityMockOverwritesRegular(ctx context.Context) error {
	mockOverwritesRegularCalls.Add(1)
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}
func WorkflowMockOverwritesRegular(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, ActivityMockOverwritesRegular).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_MockOverwritesRegular(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.RegisterActivity(ActivityMockOverwritesRegular)
	env.OnActivity(ActivityMockOverwritesRegular, mock.Anything).Return(nil)

	env.ExecuteWorkflow(WorkflowMockOverwritesRegular)
	require.True(t, env.IsWorkflowCompleted())

	// The mock runs instead of the real activity, in both places.
	require.Equal(t, int32(0), mockOverwritesRegularCalls.Load())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowMockOverwritesRegular", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityMockOverwritesRegular", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

// === Makes sure that using a named registration without an OnActivity mock
// actually uses the proper function instead of the mock (added to capture this
// bug I was having)

const NamedRegistrationActivityName = "NamedRegistration"

var namedRegistrationCalls atomic.Int32

func ActivityNamedRegistration(ctx context.Context) error {
	namedRegistrationCalls.Add(1)
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}
func WorkflowNamedRegistration(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, NamedRegistrationActivityName).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_NamedRegistration(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.RegisterActivityWithOptions(ActivityNamedRegistration, activity.RegisterOptions{
		Name: NamedRegistrationActivityName,
	})
	env.ExecuteWorkflow(WorkflowNamedRegistration)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// The real activity runs once in the test environment and once on the dev
	// server.
	require.Equal(t, int32(2), namedRegistrationCalls.Load())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowNamedRegistration", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, NamedRegistrationActivityName, hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

type ActivitiesStructFlow struct{}

func (a *ActivitiesStructFlow) ActivityStructFlow(ctx context.Context) error {
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}
func WorkflowStructFlow(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	var a *ActivitiesStructFlow
	if err := workflow.ExecuteActivity(ctx, a.ActivityStructFlow).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

// === Test that it works properly with a struct registration.

func TestEnvironment_StructFlow(t *testing.T) {

	var a *ActivitiesStructFlow
	testworker.RegisterWorkflow(WorkflowStructFlow)
	testworker.RegisterActivity(&ActivitiesStructFlow{})
	_, err := testclient.ExecuteWorkflow(t.Context(), tclient.StartWorkflowOptions{
		ID:        "Test Struct Flow",
		TaskQueue: REAL_EXECUTION_TASK_QUEUE,
	}, WorkflowStructFlow)
	require.Nil(t, err)

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(a.ActivityStructFlow, mock.Anything).Return(nil)

	env.ExecuteWorkflow(WorkflowStructFlow)
	require.True(t, env.IsWorkflowCompleted())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowStructFlow", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "ActivityStructFlow", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

// === Test that mocking one method of a registered struct leaves the other
// methods running the real implementation.

var structPartiallyMockedMockedCalls atomic.Int32
var structPartiallyMockedRealCalls atomic.Int32

type ActivitiesStructPartiallyMocked struct{}

func (a *ActivitiesStructPartiallyMocked) MockedActivityStructPartiallyMocked(ctx context.Context) error {
	structPartiallyMockedMockedCalls.Add(1)
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}

func (a *ActivitiesStructPartiallyMocked) RealActivityStructPartiallyMocked(ctx context.Context) error {
	structPartiallyMockedRealCalls.Add(1)
	name := activity.GetInfo(ctx).ActivityType.Name
	activity.GetLogger(ctx).Info(name)
	return nil
}

func WorkflowStructPartiallyMocked(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	var a *ActivitiesStructPartiallyMocked
	if err := workflow.ExecuteActivity(ctx, a.MockedActivityStructPartiallyMocked).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	if err := workflow.ExecuteActivity(ctx, a.RealActivityStructPartiallyMocked).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

func TestEnvironment_StructPartiallyMocked(t *testing.T) {

	var a *ActivitiesStructPartiallyMocked

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.RegisterActivity(&ActivitiesStructPartiallyMocked{})
	env.OnActivity(a.MockedActivityStructPartiallyMocked, mock.Anything).Return(nil)

	env.ExecuteWorkflow(WorkflowStructPartiallyMocked)
	require.True(t, env.IsWorkflowCompleted())

	// The mocked method never runs, the other one runs once in the test
	// environment and once on the dev server.
	require.Equal(t, int32(0), structPartiallyMockedMockedCalls.Load())
	require.Equal(t, int32(2), structPartiallyMockedRealCalls.Load())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowStructPartiallyMocked", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "MockedActivityStructPartiallyMocked", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
	require.Equal(t, "RealActivityStructPartiallyMocked", hist.Events[10].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

// === Test that timers are recorded as events on the dev server. Also using
// a long timer duration to show that it's overwritten to a neglijible one

func WorkflowTimer(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	return workflow.NewTimer(ctx, 1*time.Hour).Get(ctx, nil)
}

func TestEnvironment_Timer(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)

	env.ExecuteWorkflow(WorkflowTimer)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowTimer", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	// check that the interceptor did override
	require.Equal(t, shortRetryInterval, hist.Events[4].GetTimerStartedEventAttributes().GetStartToFireTimeout().AsDuration())
}

// === Test that sleeps are recorded as timer events on the dev server.

func WorkflowSleep(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	return workflow.Sleep(ctx, 1*time.Hour)
}

func TestEnvironment_Sleep(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)

	env.ExecuteWorkflow(WorkflowSleep)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowSleep", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, shortRetryInterval, hist.Events[4].GetTimerStartedEventAttributes().GetStartToFireTimeout().AsDuration())
}

// === Test that zero timers are not recorded as events on the dev server. This
// I think is specific to the Go SDK and means changing a timer's duration from
// a positive number to zero will cause NDE.

func WorkflowZeroTimer(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	return workflow.NewTimer(ctx, 0).Get(ctx, nil)
}

func TestEnvironment_ZeroTimer(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)

	env.ExecuteWorkflow(WorkflowZeroTimer)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowZeroTimer", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
}

// === Same as above but with sleep.

func WorkflowZeroSleep(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	return workflow.Sleep(ctx, 0)
}

func TestEnvironment_ZeroSleep(t *testing.T) {

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)

	env.ExecuteWorkflow(WorkflowZeroSleep)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	require.Equal(t, "WorkflowZeroSleep", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
}

const NamedActivityName = "NamedActivity"

func WorkflowNamedActivity(ctx workflow.Context) error {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	logger := workflow.GetLogger(ctx)
	logger.Info(name)

	ctx = workflow.WithActivityOptions(
		ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: 1 * time.Minute,
		},
	)

	if err := workflow.ExecuteActivity(ctx, NamedActivityName).Get(ctx, nil); err != nil {
		logger.Error("Execute Activity failed.", "Error", err)
		return err
	}
	return nil
}

// TODO: this test currently fails, need to figure out how to handle aliasing.
// func TestEnvironment_NamedActivity(t *testing.T) {

// 	Activity := func(ctx context.Context) error {
// 		name := activity.GetInfo(ctx).ActivityType.Name
// 		activity.GetLogger(ctx).Info(name)
// 		return nil
// 	}

// 	testworker.RegisterWorkflow(WorkflowNamedActivity)
// 	testworker.RegisterActivityWithOptions(Activity, activity.RegisterOptions{Name: NamedActivityName})
// 	_, err := testclient.ExecuteWorkflow(t.Context(), tclient.StartWorkflowOptions{
// 		ID:        "Test Named Activity",
// 		TaskQueue: REAL_EXECUTION_TASK_QUEUE,
// 	}, WorkflowNamedActivity)
// 	require.Nil(t, err)

// 	s := testsuite.WorkflowTestSuite{}
// 	env := TestWorkflowEnvironment{
// 		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
// 		tclient:                 testclient,
// 		dumpHistory:             dumpSingleHistory,
// 		activityStubFnRef:       map[string]any{},
// 		workflowStubFnRef:       map[string]any{},
// 	}
// 	env.RegisterActivityWithOptions(Activity, activity.RegisterOptions{Name: NamedActivityName})
// 	env.OnActivity(NamedActivityName, mock.Anything).Return(nil)

// 	env.ExecuteWorkflow(WorkflowNamedActivity)
// 	require.True(t, env.IsWorkflowCompleted())

// 	hist := env.workflowHistoryResult
// 	require.NotNil(t, hist)

// 	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
// 	for _, ev := range hist.Events {
// 		eventTypes = append(eventTypes, ev.GetEventType())
// 	}
// 	require.Equal(t, []enumspb.EventType{
// 		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
// 		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
// 		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
// 		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
// 		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
// 		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
// 	}, eventTypes)

// 	require.Equal(t, "WorkflowNamedActivity", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
// 	require.Equal(t, NamedActivityName, hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
// }

func TestEnvironment_NamesAsClosures(t *testing.T) {

	Activity := func(ctx context.Context) error {
		activity.GetLogger(ctx).Info("ActivityNamesAsClosures")
		return nil
	}
	Workflow := func(ctx workflow.Context) error {
		logger := workflow.GetLogger(ctx)
		logger.Info("WorkflowNamesAsClosures")

		ctx = workflow.WithActivityOptions(
			ctx, workflow.ActivityOptions{
				ScheduleToCloseTimeout: 1 * time.Minute,
			},
		)

		if err := workflow.ExecuteActivity(ctx, Activity).Get(ctx, nil); err != nil {
			logger.Error("Activity execution failed.", "Error", err)
			return err
		}
		return nil
	}

	testworker.RegisterWorkflow(Workflow)
	testworker.RegisterActivity(Activity)
	// testworker.RegisterActivity(Activity)
	_, err := testclient.ExecuteWorkflow(t.Context(), tclient.StartWorkflowOptions{
		ID:        "Test Names as Closures",
		TaskQueue: REAL_EXECUTION_TASK_QUEUE,
	}, Workflow)
	require.Nil(t, err)

	s := testsuite.WorkflowTestSuite{}
	env := newTestEnv(s)
	env.OnActivity(Activity, mock.Anything).Return(nil)

	env.ExecuteWorkflow(Workflow)
	require.True(t, env.IsWorkflowCompleted())

	hist := env.workflowHistoryResult
	require.NotNil(t, hist)

	eventTypes := make([]enumspb.EventType, 0, len(hist.Events))
	for _, ev := range hist.Events {
		eventTypes = append(eventTypes, ev.GetEventType())
	}
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}, eventTypes)

	// This is the name inferred for anonymous functions.
	require.Equal(t, "func2", hist.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName())
	require.Equal(t, "func1", hist.Events[4].GetActivityTaskScheduledEventAttributes().GetActivityType().GetName())
}

func plainActivityForNameTest() error {
	return nil
}

type activityFunctionNameTestActivities struct{}

func (activityFunctionNameTestActivities) ValueReceiverActivity() error {
	return nil
}

func (*activityFunctionNameTestActivities) PointerReceiverActivity() error {
	return nil
}

// basically making sure to keep this in sync with the SDK since I need it to
// generate string name identifiers for stubs
//
//go:linkname getFunctionNameSDK go.temporal.io/sdk/internal.getFunctionName
func getFunctionNameSDK(i any) (name string, isMethod bool)

func requireSameAsSDK(t *testing.T, i any, name string, isMethod bool) {
	t.Helper()
	sdkName, sdkIsMethod := getFunctionNameSDK(i)
	if name != sdkName || isMethod != sdkIsMethod {
		t.Fatalf("got (%q, %v), SDK returns (%q, %v)", name, isMethod, sdkName, sdkIsMethod)
	}
}

func TestActivityFunctionName(t *testing.T) {
	t.Run("string activity name", func(t *testing.T) {
		// Given
		const activityName = "CustomActivityName"

		// When
		name, isMethod := getFunctionName(activityName)

		// Then
		if name != activityName {
			t.Fatalf("name = %q, want %q", name, activityName)
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
		requireSameAsSDK(t, activityName, name, isMethod)
	})

	t.Run("plain function", func(t *testing.T) {
		// Given
		activityFn := plainActivityForNameTest

		// When
		name, isMethod := getFunctionName(activityFn)

		// Then
		if name != "plainActivityForNameTest" {
			t.Fatalf("name = %q, want %q", name, "plainActivityForNameTest")
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
		requireSameAsSDK(t, activityFn, name, isMethod)
	})

	t.Run("value receiver method value", func(t *testing.T) {
		// Given
		activities := activityFunctionNameTestActivities{}

		// When
		name, isMethod := getFunctionName(activities.ValueReceiverActivity)

		// Then
		if name != "ValueReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "ValueReceiverActivity")
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
		requireSameAsSDK(t, activities.ValueReceiverActivity, name, isMethod)
	})

	t.Run("pointer receiver method value", func(t *testing.T) {
		// Given
		activities := &activityFunctionNameTestActivities{}

		// When
		name, isMethod := getFunctionName(activities.PointerReceiverActivity)

		// Then
		if name != "PointerReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "PointerReceiverActivity")
		}
		if !isMethod {
			t.Fatal("isMethod = false, want true")
		}
		requireSameAsSDK(t, activities.PointerReceiverActivity, name, isMethod)
	})

	t.Run("nil pointer receiver method value", func(t *testing.T) {
		// Given
		var activities *activityFunctionNameTestActivities

		// When
		name, isMethod := getFunctionName(activities.PointerReceiverActivity)

		// Then
		if name != "PointerReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "PointerReceiverActivity")
		}
		if !isMethod {
			t.Fatal("isMethod = false, want true")
		}
		requireSameAsSDK(t, activities.PointerReceiverActivity, name, isMethod)
	})

	t.Run("anonymous function", func(t *testing.T) {
		// Given
		activityFn := func() error {
			return nil
		}

		// When
		name, isMethod := getFunctionName(activityFn)

		// Then
		if name != "1" {
			t.Fatalf("name = %q, want %q", name, "1")
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
		requireSameAsSDK(t, activityFn, name, isMethod)
	})
}

func TestAnnotatePayloadData(t *testing.T) {
	raw := []byte(`{
		"events": [
			{
				"payloads": [
					{
						"metadata": {
							"encoding": "anNvbi9wbGFpbg=="
						},
						"data": "InZhbHVlOjci"
					},
					{
						"metadata": {
							"encoding": "anNvbi9wbGFpbg=="
						},
						"data": "Nw=="
					},
					{
						"metadata": {
							"encoding": "YmluYXJ5L251bGw="
						}
					}
				]
			}
		]
	}`)

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	annotatePayloadData(value)
	annotated, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal annotated: %v", err)
	}

	var decoded struct {
		Events []struct {
			Payloads []struct {
				Data        string          `json:"data"`
				DecodedData json.RawMessage `json:"__replaysuite_decodedData"`
			} `json:"payloads"`
		} `json:"events"`
	}
	if err := json.Unmarshal(annotated, &decoded); err != nil {
		t.Fatalf("annotated JSON did not unmarshal: %v", err)
	}

	if got := string(decoded.Events[0].Payloads[0].DecodedData); got != `"value:7"` {
		t.Fatalf("first %s = %s, want %s", decodedPayloadDataJSONKey, got, `"value:7"`)
	}
	if got := string(decoded.Events[0].Payloads[1].DecodedData); got != `7` {
		t.Fatalf("second %s = %s, want 7", decodedPayloadDataJSONKey, got)
	}
	if got := string(decoded.Events[0].Payloads[2].DecodedData); got != `null` {
		t.Fatalf("third %s = %s, want null", decodedPayloadDataJSONKey, got)
	}
	if decoded.Events[0].Payloads[0].Data != "InZhbHVlOjci" {
		t.Fatal("original payload data was not preserved")
	}
}
