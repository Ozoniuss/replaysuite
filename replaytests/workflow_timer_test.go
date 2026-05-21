package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/suite"
)

type WorkflowTimerTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowTimerTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTimerTestSuite))
}

func (s *WorkflowTimerTestSuite) SetupSuite() {
	// Register the timer workflow for replay tests.
	s.RegisterWorkflowForReplay(WorkflowWithTimer)
	s.RegisterWorkflowForReplay(WorkflowWithZeroTimer)
	s.BaseTestSuite.SetupSuite()
}

func (s *WorkflowTimerTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment()
}

func (s *WorkflowTimerTestSuite) Test_WorkflowWithTimer() {
	s.env.ExecuteWorkflow(WorkflowWithTimer)
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkflowTimerTestSuite) Test_WorkflowWithZeroTimer() {
	s.env.ExecuteWorkflow(WorkflowWithZeroTimer)
	s.True(s.env.IsWorkflowCompleted())
}

/*
Now define a comprehensive test suite that cover all branches of your workflow.
This should give you enough histories for replay tests.
*/
