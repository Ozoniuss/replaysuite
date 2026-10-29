package regulartests

import (
	"context"
	"sync/atomic"
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

func Test_Workflow_MockingFuncRegisteredWithCustomNameMocksCustomName(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(EchoName, activity.RegisterOptions{
		Name: "AliasedEchoName",
	})

	env.OnActivity(EchoName, mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(GoesToBranch1Activity, mock.Anything).Return()
	env.OnActivity(GoesToBranch2Activity, mock.Anything).Return()

	env.ExecuteWorkflow(Workflow, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 0, result.B1cnt)
	assert.Equal(t, 10, result.B2cnt)
	env.AssertActivityNumberOfCalls(t, "AliasedEchoName", 10)
	env.AssertActivityNotCalled(t, "EchoName", mock.Anything, mock.Anything)
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

func Test_Workflow_DoNotRegisterStructButMockOnlyASubsetOfMethods(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{}

	// mocking a method only registers that method, not the whole struct
	env.OnActivity(a.EchoName, mock.Anything, Branch1Word).Return("Other word", nil)

	env.ExecuteWorkflow(WorkflowWithStruct, Branch1Word)

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "unable to find activityType=GoesToBranch2Activity")
	env.AssertActivityNumberOfCalls(t, "EchoName", 1)
}

func Test_Workflow_OnlyMocksStructs(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{}
	env.RegisterActivity(a)

	var branch2Runs atomic.Int32

	env.OnActivity(a.EchoName, mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity(a.GoesToBranch2Activity, mock.Anything).Return(func(context.Context) error {
		branch2Runs.Add(1)
		return nil
	})

	env.ExecuteWorkflow(WorkflowWithStruct, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 0, result.B1cnt)
	assert.Equal(t, 10, result.B2cnt)
	env.AssertActivityNumberOfCalls(t, "EchoName", 10)
	assert.Equal(t, int32(10), branch2Runs.Load())
}

func Test_Workflow_CanMockStructMethodsViaName(t *testing.T) {
	t.Parallel()
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{})

	var branch2Runs atomic.Int32

	// struct methods are registered under the method name only, without the
	// struct type
	env.OnActivity("EchoName", mock.Anything, Branch1Word).Return("Other word", nil)
	env.OnActivity("GoesToBranch2Activity", mock.Anything).Return(func(context.Context) error {
		branch2Runs.Add(1)
		return nil
	})

	env.ExecuteWorkflow(WorkflowWithStruct, Branch1Word)

	time.Sleep(1 * time.Second)

	require.True(t, env.IsWorkflowCompleted())
	var result BranchResult
	require.Nil(t, env.GetWorkflowResult(&result))

	assert.Equal(t, 0, result.B1cnt)
	assert.Equal(t, 10, result.B2cnt)
	env.AssertActivityNumberOfCalls(t, "EchoName", 10)
	assert.Equal(t, int32(10), branch2Runs.Load())
}
