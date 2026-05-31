package replaytests

import (
	"errors"
	"testing"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type WorkflowWithActivityCustomRetryPolicyTestSuite struct {
	BaseTestSuite
	env *replaysuite.Env
}

func TestWorkflowWithActivityCustomRetryPolicyTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowWithActivityCustomRetryPolicyTestSuite))
}

func (s *WorkflowWithActivityCustomRetryPolicyTestSuite) SetupSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Start(
		replaysuite.WithReplayWorkflows(WorkflowWithActivityCustomRetryPolicy),
	))
}

func (s *WorkflowWithActivityCustomRetryPolicyTestSuite) SetupTest() {
	s.env = s.NewDevServerEnvironment(s.T())
}

func (s *WorkflowWithActivityCustomRetryPolicyTestSuite) Test_WorkflowWithActivityCustomRetryPolicy() {
	// force an error to trigger the retry policy
	s.env.OnActivity(ActivityWithCustomRetryPolicy, mock.Anything, "Temporal").Return(nil, errors.New("error"))
	s.env.ExecuteWorkflow(WorkflowWithActivityCustomRetryPolicy)

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}
