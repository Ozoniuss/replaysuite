package replaytests

import (
	"testing"
	"time"

	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// todo:
// this setup doesn't write histories in time
// always goes to branch 1
// figure out activity options
func Test_WithSameActivityCalledTwice_MultipleMocksWork(t *testing.T) {
	t.Parallel()
	s := replaysuite.WorkflowTestSuite{}
	err := s.Start(replaysuite.WithReplayWorkflows(WorkflowWithSameActivityCalledTwice))
	if err != nil {
		panic(err)
	}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(GreetActivity)
	env.RegisterActivity(GoesToBranch1Activity)
	env.RegisterActivity(GoesToBranch2Activity)

	env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal!", nil)
	env.OnActivity(GreetActivity, mock.Anything, "Temporal").Return("Hello Temporal 2!", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(WorkflowWithSameActivityCalledTwice, "Temporal")

	require.True(t, env.IsWorkflowCompleted())

	time.Sleep(1 * time.Second)

	err = s.Stop()
	if err != nil {
		panic(err)
	}
}
