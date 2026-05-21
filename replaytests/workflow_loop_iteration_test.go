package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type WorkflowLoopIterationTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowLoopIterationTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowLoopIterationTestSuite))
}

func (s *WorkflowLoopIterationTestSuite) SetupSuite() {
	s.RegisterWorkflowForReplay(WorkflowWithLoopIterationChange)
	s.BaseTestSuite.SetupSuite()
}

func (s *WorkflowLoopIterationTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment()
}

func (s *WorkflowLoopIterationTestSuite) Test_WorkflowWithLoopIterationChange() {
	s.env.OnActivity(LoopIterationActivity, mock.Anything, 0).Return("iteration-0", nil)
	s.env.OnActivity(LoopIterationActivity, mock.Anything, 1).Return("iteration-1", nil)
	// When switching iterationCount to 3, add the third activity stub too:
	// s.env.OnActivity(LoopIterationActivity, mock.Anything, 2).Return("iteration-2", nil)

	s.env.ExecuteWorkflow(WorkflowWithLoopIterationChange)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	var result []string
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal([]string{"iteration-0", "iteration-1"}, result)
}
