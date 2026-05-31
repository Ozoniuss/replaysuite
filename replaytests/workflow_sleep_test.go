package replaytests

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/suite"
)

type WorkflowSleepTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowSleepTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowSleepTestSuite))
}

func (s *WorkflowSleepTestSuite) SetupSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Start(
		replaysuite.WithReplayWorkflows(WorkflowWithSleep, WorkflowWithZeroSleep),
	))
}

func (s *WorkflowSleepTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment(s.T())
}

func (s *WorkflowSleepTestSuite) Test_WorkflowWithSleep() {
	s.env.ExecuteWorkflow(WorkflowWithSleep)
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkflowSleepTestSuite) Test_WorkflowWithZeroSleep() {
	s.env.ExecuteWorkflow(WorkflowWithZeroSleep)
	s.True(s.env.IsWorkflowCompleted())
}
