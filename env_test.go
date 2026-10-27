package replaysuite

import (
	"context"
	"encoding/json"
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
	env := TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		activityStubFnRef:       map[string]any{},
		workflowStubFnRef:       map[string]any{},
	}
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
	env := TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		activityStubFnRef:       map[string]any{},
		workflowStubFnRef:       map[string]any{},
	}
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
	env := TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		activityStubFnRef:       map[string]any{},
		workflowStubFnRef:       map[string]any{},
	}
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
	env := TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		activityStubFnRef:       map[string]any{},
		workflowStubFnRef:       map[string]any{},
	}
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
	env := TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		tclient:                 testclient,
		dumpHistory:             dumpSingleHistory,
		activityStubFnRef:       map[string]any{},
		workflowStubFnRef:       map[string]any{},
	}
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
