package regulartests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func Test_Workflow_FirstMockWins(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(EchoName)

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return(Branch1Word, nil)
	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 10, result.B1cnt)
	assert.Equal(t, 0, result.B2cnt)
}

func Test_Workflow_MocksTakeTurnsWithTimes(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(EchoName)

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 3, result.B1cnt)
	assert.Equal(t, 7, result.B2cnt)
}

func Test_Workflow_MocksWorkTheSameWithoutRegistration(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 3, result.B1cnt)
	assert.Equal(t, 7, result.B2cnt)
}

func Test_Workflow_CanMockViaStringWhenRegistered(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(EchoName)

	env.OnActivity("EchoName", mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	env.OnActivity("EchoName", mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 3, result.B1cnt)
	assert.Equal(t, 7, result.B2cnt)
}

func Test_Workflow_PanicsIfItRegistersWithString(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	require.Panics(t, func() {
		env.RegisterActivity("EchoName")
	})
}

func Test_Workflow_PanicsIfItRegistersWithStringEvenIfPreviouslyRegisterdAsFunc(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(EchoName)

	require.Panics(t, func() {
		env.RegisterActivity("EchoName")
	})
}

func Test_Workflow_PanicsIfItRegistersWithStringWithOpts(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	require.Panics(t, func() {
		env.RegisterActivityWithOptions("EchoName", activity.RegisterOptions{})
	})
}

func Test_Workflow_MustCallRegisterFirstIfUsingString(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	require.Panics(t, func() {
		env.OnActivity("EchoName", mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	})
}

func Test_Workflow_CanMockViaStringWhenRegisteringAnyFunctionWithMatchingSignature(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(context.Context, string) (string, error) { return "", nil }, activity.RegisterOptions{
		Name: "EchoName",
	})

	env.OnActivity("EchoName", mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	env.OnActivity("EchoName", mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 3, result.B1cnt)
	assert.Equal(t, 7, result.B2cnt)
}

func Test_Workflow_DuplicateRegistrationOverwritesImplementation(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	ret := 0
	env.RegisterActivityWithOptions(func(context.Context, string) (string, error) { return "", nil }, activity.RegisterOptions{
		Name: "EchoName",
	})
	env.RegisterActivityWithOptions(func(context.Context, string) (string, error) {
		ret = 1
		return "", nil
	}, activity.RegisterOptions{
		Name: "EchoName",
	})

	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, 1, ret)
}

func Test_Workflow_DuplicateRegistrationOverwritesImplementationEvenWhenMixed(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	ret := 0
	env.RegisterActivityWithOptions(func(context.Context, string) (string, error) {
		ret = 1
		return "", nil
	}, activity.RegisterOptions{
		Name: "EchoName",
	})
	env.RegisterActivity(EchoName)

	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, 0, ret)
}

func Test_Workflow_CanMockWithFuncEvenWhenRegisteringAsString(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	ret := 0
	env.RegisterActivityWithOptions(func(context.Context, string) (string, error) {
		ret = 1
		return "", nil
	}, activity.RegisterOptions{
		Name: "EchoName",
	})

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return(Branch1Word, nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, 0, ret)
}

// need a registration first if I use a named activity

func Test_Workflow_CannotMockViaStringIfNotRegistered(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	require.Panics(t, func() {
		env.OnActivity("EchoName", mock.Anything, Branch1Word).Return(Branch1Word, nil)
	})
}

func Test_Workflow_MockDoesNotFallbackOnRealExecution(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	// even if EchoName is registered, mock will take preference and panic after
	// 3 executions
	env.RegisterActivity(EchoName)

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return(Branch1Word, nil).Times(3)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "The method has been called over 3 times")
}
