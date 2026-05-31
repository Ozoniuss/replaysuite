package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
)

type WorkflowTestSuite struct {
	suite.Suite
	// Import replay suite as a replacement for the test workflow suite. This
	// will run the regular unit tests, but on top of that it will also emit
	// event histories for each test, as well as do replay testing for each
	// workflow that is registered for that.
	replaysuite.WorkflowTestSuite
	env *replaysuite.Env
}

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

func (s *WorkflowTestSuite) SetupSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Start(
		replaysuite.WithReplayWorkflows(Workflow),
		replaysuite.WithRedactWorkerIdentity(),
	))
}

func (s *WorkflowTestSuite) TearDownSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Stop())
}

func (s *WorkflowTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment(s.T())
	// ChildWorkflow is invoked by name via ExecuteChildWorkflow, so it must be
	// registered explicitly (the parent Workflow auto-registers on execute).
	s.env.RegisterWorkflow(ChildWorkflow)
}

/*
Now define a comprehensive test suite that cover all branches of your workflow.
This should give you enough histories for replay tests.
*/

func (s *WorkflowTestSuite) Test_Workflow_EmptyName() {
	s.env.ExecuteWorkflow(Workflow, "")

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) Test_Workflow_LongBranch() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	s.env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").Return("long-handled(Hello Temporal!)", nil)
	s.env.OnActivity(UppercaseActivity, mock.Anything, "Hello Temporal!").Return("HELLO TEMPORAL!", nil)
	s.env.OnActivity(ReverseActivity, mock.Anything, "Hello Temporal!").Return("!laropmeT olleH", nil)

	s.env.ExecuteWorkflow(Workflow, "Temporal")

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal("[long:long-handled(Hello Temporal!)] HELLO TEMPORAL! | !laropmeT olleH", result)
}

func (s *WorkflowTestSuite) Test_Workflow_ShortBranch() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Bo").Return("Hello Bo!", nil)
	s.env.OnWorkflow(ChildWorkflow, mock.Anything, "Hello Bo!").Return("Hello Bo!", nil)
	s.env.OnActivity(UppercaseActivity, mock.Anything, "Hello Bo!").Return("HELLO BO!", nil)
	s.env.OnActivity(ReverseActivity, mock.Anything, "Hello Bo!").Return("!oB olleH", nil)

	s.env.ExecuteWorkflow(Workflow, "Bo")

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	// branchResult comes from ChildWorkflow, which echoes the greeting.
	s.Equal("[short:Hello Bo!] HELLO BO! | !oB olleH", result)
}

func (s *WorkflowTestSuite) Test_Workflow_GreetActivityFails() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Temporal").
		Return("", temporal.NewNonRetryableApplicationError("greet failed", "GreetError", nil))

	s.env.ExecuteWorkflow(Workflow, "Temporal")

	s.True(s.env.IsWorkflowCompleted())
	s.ErrorContains(s.env.GetWorkflowError(), "greet failed")
}

func (s *WorkflowTestSuite) Test_Workflow_BranchActivityFails() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	s.env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").
		Return("", temporal.NewNonRetryableApplicationError("long path failed", "LongPathError", nil))

	s.env.ExecuteWorkflow(Workflow, "Temporal")

	s.True(s.env.IsWorkflowCompleted())
	s.ErrorContains(s.env.GetWorkflowError(), "long path failed")
}

func (s *WorkflowTestSuite) Test_Workflow_ChildWorkflowFails() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Bo").Return("Hello Bo!", nil)
	s.env.OnWorkflow(ChildWorkflow, mock.Anything, "Hello Bo!").
		Return("", temporal.NewNonRetryableApplicationError("child failed", "ChildError", nil))

	s.env.ExecuteWorkflow(Workflow, "Bo")

	s.True(s.env.IsWorkflowCompleted())
	s.ErrorContains(s.env.GetWorkflowError(), "child failed")
}

func (s *WorkflowTestSuite) Test_Workflow_ParallelActivityFails() {
	s.env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	s.env.OnActivity(LongPathActivity, mock.Anything, "Hello Temporal!").Return("long-handled(Hello Temporal!)", nil)
	s.env.OnActivity(UppercaseActivity, mock.Anything, "Hello Temporal!").
		Return("", temporal.NewNonRetryableApplicationError("uppercase failed", "UppercaseError", nil))
	s.env.OnActivity(ReverseActivity, mock.Anything, "Hello Temporal!").Return("!laropmeT olleH", nil)

	s.env.ExecuteWorkflow(Workflow, "Temporal")

	s.True(s.env.IsWorkflowCompleted())
	s.ErrorContains(s.env.GetWorkflowError(), "uppercase failed")
}
