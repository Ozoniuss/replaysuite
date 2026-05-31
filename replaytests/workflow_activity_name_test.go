package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type WorkflowActivityNameTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowActivityNameTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowActivityNameTestSuite))
}

func (s *WorkflowActivityNameTestSuite) SetupSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Start(
		replaysuite.WithReplayWorkflows(WorkflowWithActivityNameChange),
	))
}

func (s *WorkflowActivityNameTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment(s.T())
}

func (s *WorkflowActivityNameTestSuite) Test_WorkflowWithActivityNameChange() {
	s.env.OnActivity(OriginalNameActivity, mock.Anything, "Temporal").Return("hello Temporal", nil)
	// When switching the workflow to RenamedActivity, update the stub too:
	// s.env.OnActivity(RenamedActivity, mock.Anything, "Temporal").Return("hello Temporal", nil)

	s.env.ExecuteWorkflow(WorkflowWithActivityNameChange)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal("hello Temporal", result)
}
