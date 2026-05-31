package replaytests

import (
	"os"
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

/*
	Example without testify.
*/

var testMainSuite replaysuite.WorkflowTestSuite

func TestMain(m *testing.M) {
	if err := testMainSuite.Start(
		replaysuite.WithReplayWorkflows(Workflow),
		replaysuite.WithRedactWorkerIdentity(),
	); err != nil {
		panic(err)
	}
	code := m.Run()
	if err := testMainSuite.Stop(); err != nil {
		panic(err)
	}
	os.Exit(code)
}

func newEnv() *replaysuite.Env {
	env := testMainSuite.NewTestWorkflowEnvironment()
	// ChildWorkflow is invoked by name via ExecuteChildWorkflow, so it must be
	// registered explicitly (the parent Workflow auto-registers on execute).
	env.RegisterWorkflow(ChildWorkflow)
	return env
}

func Test_Workflow_EmptyName(t *testing.T) {
	env := newEnv()

	env.ExecuteWorkflow(Workflow, "")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func Test_Workflow_LongBranch(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").Return("long-handled(Hello Temporal!)", nil)
	env.OnActivity(UppercaseActivity, mock.Anything, "Hello Temporal!").Return("HELLO TEMPORAL!", nil)
	env.OnActivity(ReverseActivity, mock.Anything, "Hello Temporal!").Return("!laropmeT olleH", nil)

	env.ExecuteWorkflow(Workflow, "Temporal")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "[long:long-handled(Hello Temporal!)] HELLO TEMPORAL! | !laropmeT olleH", result)
}

func Test_Workflow_ShortBranch(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Bo").Return("Hello Bo!", nil)
	env.OnWorkflow(ChildWorkflow, mock.Anything, "Hello Bo!").Return("Hello Bo!", nil)
	env.OnActivity(UppercaseActivity, mock.Anything, "Hello Bo!").Return("HELLO BO!", nil)
	env.OnActivity(ReverseActivity, mock.Anything, "Hello Bo!").Return("!oB olleH", nil)

	env.ExecuteWorkflow(Workflow, "Bo")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "[short:Hello Bo!] HELLO BO! | !oB olleH", result)
}

func Test_Workflow_GreetActivityFails(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Temporal").
		Return("", temporal.NewNonRetryableApplicationError("greet failed", "GreetError", nil))

	env.ExecuteWorkflow(Workflow, "Temporal")

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "greet failed")
}

func Test_Workflow_BranchActivityFails(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").
		Return("", temporal.NewNonRetryableApplicationError("long path failed", "LongPathError", nil))

	env.ExecuteWorkflow(Workflow, "Temporal")

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "long path failed")
}

func Test_Workflow_ChildWorkflowFails(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Bo").Return("Hello Bo!", nil)
	env.OnWorkflow(ChildWorkflow, mock.Anything, "Hello Bo!").
		Return("", temporal.NewNonRetryableApplicationError("child failed", "ChildError", nil))

	env.ExecuteWorkflow(Workflow, "Bo")

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "child failed")
}

func Test_Workflow_ParallelActivityFails(t *testing.T) {
	env := newEnv()
	env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").Return("long-handled(Hello Temporal!)", nil)
	env.OnActivity(UppercaseActivity, mock.Anything, "Hello Temporal!").
		Return("", temporal.NewNonRetryableApplicationError("uppercase failed", "UppercaseError", nil))
	env.OnActivity(ReverseActivity, mock.Anything, "Hello Temporal!").Return("!laropmeT olleH", nil)

	env.ExecuteWorkflow(Workflow, "Temporal")

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "uppercase failed")
}
