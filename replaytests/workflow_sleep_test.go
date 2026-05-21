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
	// Register the timer workflow for replay tests.
	s.RegisterWorkflowForReplay(WorkflowWithSleep)
	s.RegisterWorkflowForReplay(WorkflowWithZeroSleep)
	s.BaseTestSuite.SetupSuite()
}

func (s *WorkflowSleepTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment()
}

func (s *WorkflowSleepTestSuite) Test_WorkflowWithSleep() {
	s.env.ExecuteWorkflow(WorkflowWithSleep)
	s.True(s.env.IsWorkflowCompleted())
}

func (s *WorkflowSleepTestSuite) Test_WorkflowWithZeroSleep() {
	s.env.ExecuteWorkflow(WorkflowWithZeroSleep)
	s.True(s.env.IsWorkflowCompleted())
}
