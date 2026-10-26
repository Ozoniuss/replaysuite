package replaysuite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

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

	v, ok := os.LookupEnv("REPLAYSUITE_TEST_WITH_REAL_SERVER")
	var srv *testsuite.DevServer

	if ok && v == "true" {
		// allow developing with a real server to compare histories
		testclient, err = tclient.Dial(tclient.Options{
			HostPort: "localhost:7233",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to existing dev server %s", err.Error())
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "using real server")

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

func TestEnvironment(t *testing.T) {

	Activity := func(ctx context.Context) error {
		activity.GetLogger(ctx).Info("Activity")
		return nil
	}
	Workflow := func(ctx workflow.Context) error {
		logger := workflow.GetLogger(ctx)
		logger.Info("Workflow")

		ctx = workflow.WithActivityOptions(
			ctx, workflow.ActivityOptions{
				ScheduleToCloseTimeout: 1 * time.Minute,
			},
		)

		if err := workflow.ExecuteActivity(ctx, Activity).Get(ctx, nil); err != nil {
			logger.Error("GreetActivity failed.", "Error", err)
			return err
		}
		return nil
	}

	testworker.RegisterWorkflow(Workflow)
	testworker.RegisterActivity(Activity)
	_, err := testclient.ExecuteWorkflow(t.Context(), tclient.StartWorkflowOptions{
		ID:        "Test 1",
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
