package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type WorkflowActivityIOTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowActivityIOTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowActivityIOTestSuite))
}

func (s *WorkflowActivityIOTestSuite) SetupSuite() {
	// Register the activity I/O workflow for replay tests.
	s.RegisterWorkflowForReplay(WorkflowWithInputChangeActivity)
	s.BaseTestSuite.SetupSuite()
}

func (s *WorkflowActivityIOTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment()
}

func (s *WorkflowActivityIOTestSuite) Test_WorkflowWithActivityInputStringToInt() {
	// s.env.OnActivity(ActivityInputActivity, mock.Anything, "number 7").Return("value:7", nil)
	// When changing the activity input from string to int, update the stub to:
	s.env.OnActivity(InputChangeActivity, mock.Anything, 7).Return("value:7", nil)

	s.env.ExecuteWorkflow(WorkflowWithInputChangeActivity)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal("value:7", result)
}

/*
Now define a comprehensive test suite that cover all branches of your workflow.
This should give you enough histories for replay tests.
*/
