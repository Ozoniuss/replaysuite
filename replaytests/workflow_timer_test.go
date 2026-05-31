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
	s.Require().NoError(s.WorkflowTestSuite.Start(
		replaysuite.WithReplayWorkflows(WorkflowWithTimer, WorkflowWithZeroTimer),
	))
}

func (s *WorkflowTimerTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment(s.T())
}

func (s *WorkflowTimerTestSuite) Test_WorkflowWithTimer() {
	s.env.ExecuteWorkflow(WorkflowWithTimer)
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkflowTimerTestSuite) Test_WorkflowWithMultipleTimers() {
	s.env.ExecuteWorkflow(WorkflowWithMultipleTimers)
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkflowTimerTestSuite) Test_WorkflowWithZeroTimer() {
	s.env.ExecuteWorkflow(WorkflowWithZeroTimer)
	s.True(s.env.IsWorkflowCompleted())
}
