# replay-suite

> This is currently work in progress.

Generate event histories for your workflow using your unit test suite, and use
them to write replay tests.

The idea behind it is that a theoretically complete test coverage should produce
all possible event combinations that a given workflow can generate. Replay tests
targeting executions from a production cluster run the risk that the history
set may not be representative of all possible states the workflow could reach,
and also two consecutive runs may produce different results if the set varies.
This package aims to reduce that risk by providing a stable, varied set of
histories to be used in replay tests that is cheap to generate and extend with
minimal changes to your existing test suites. Obviously, in practice it's not
always possible to produce all event sequences a workflow could generate, but
a comprehensive unit test suite should provide a good enough coverage to detect
workflow changes that may potentially result in non-deterministic errors.

## Generating histories from unit tests

This package provides `replaysuite.TestWorkflowEnvironment` as a wrapper over
Temporal's `TestWorkflowEnvironment` in order to generate history files. It does
so by substituting the mocked implementation with a real one whose inputs and
outputs match the mock and which runs against a Temporal server. The environment
applies several optimizations to keep these executions short while not affecting
the replay result.

All histories will be written locally to a directory, in a format such that each
execution can be identified with a specific workflow function. By default,
replay suite will derive the workflow type from the package name. When a
workflow is explicitly registered under an alias, **it will not use the alias as
the workflow type!** This is because:

- The same workflow may be registered in different tests under different
aliases, but we still need to run the same replay tests in both cases;
- If two different workflow functions use the same alias, it will not be
possible to infer which workflow function to actually use for replay testing.

A test workflow environment will also allows explicitly setting a key that will
be used as the identifier for a specific workflow function. In that case, the
history will be stored under a directory matching that key. This is to cover
scenarios in which you want to move an existing workflow to a different package
but still be able to execute previous histories. Keep in mind that for this to
work properly, all workflow test suites that execute a specific function must 
use the same key. Note that this key is not related to the workflow alias.

Generally, it is recommended to avoid registering workflows under an alias.

## Limitations

Registering the same workflow more than once in the same test environment may
result in unpredictable behavior in how the folder gets generated. For example,
see what happens in the following test:

```go
func Test_Workflow_FuncRegisteredWithAndWithoutCustomNameKeepsBothNames(t *testing.T) {
	t.Parallel()

	registerPlain := func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterWorkflow(WorkflowTypeThatIsAliased)
	}
	registerAliased := func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterWorkflowWithOptions(WorkflowTypeThatIsAliased, workflow.RegisterOptions{
			Name: "AliasedWorkflowTypeName",
		})
	}

	orders := map[string][]func(*testsuite.TestWorkflowEnvironment){
		"plain first":   {registerPlain, registerAliased},
		"aliased first": {registerAliased, registerPlain},
	}
	executions := []struct {
		workflow any
		wantType string
	}{
		{WorkflowTypeThatIsAliased, "AliasedWorkflowTypeName"},
		{"WorkflowTypeThatIsAliased", "WorkflowTypeThatIsAliased"},
		{"AliasedWorkflowTypeName", "AliasedWorkflowTypeName"},
	}

	for order, registrations := range orders {
		for _, ex := range executions {
			s := testsuite.WorkflowTestSuite{}
			env := s.NewTestWorkflowEnvironment()
			for _, register := range registrations {
				register(env)
			}

			env.ExecuteWorkflow(ex.workflow)

			require.True(t, env.IsWorkflowCompleted(), "%s: %v", order, ex.workflow)
			var result string
			require.Nil(t, env.GetWorkflowResult(&result), "%s: %v", order, ex.workflow)
			assert.Equal(t, ex.wantType, result, "%s: %v", order, ex.workflow)
		}
	}
}
```

Replay suite will try to follow the SDK behavior in these cases (in terms of
which workflow type gets scheduled), but it does recommend to only register the 
workflow you want to test once and will only guarantee a stable behavior in that scenario.

When activities are registered with a custom name, the Temporal SDK recommends
always referring to them by that name. For such activities, replaysuite only
matches the SDK test environment's behavior if mocking them also uses the custom
name.

Note that named registrations are required if your workflow calls an activity
for which the code lives in a different location (in that case the workflow
can only invoke it via name, but it would be registered on different workers
that have access to its definition). Since the test suite can't access the
actual definition, a stub implementation matching that function's signature must
be provided under an alias. I only recommend ever using named registrations in
that case, since a mix of using strings and functions when registering and
mocking can behave quite unpredictably. I tried to keep the workflow test suite
behavior as much as I could.

To understand why aliasing is discouraged, the following workflow actually fails
when using named registration:

```go
type Activities struct {
}

func (a *Activities) GetGreeting() (string, error) {
	return "", nil
}


func GreetingSample(ctx workflow.Context) (string, error) {

    // ...
	err := workflow.ExecuteActivity(ctx, a.GetGreeting).Get(ctx, &greetResult)
	if err != nil {
		logger.Error("Get greeting failed.", "Error", err)
		return "", err
	}
    // ...
}

// worker registration 
// ...
	w.RegisterWorkflow(greetings.GreetingSample)
	activities := &greetings.Activities{Name: "Temporal", Greeting: "Hello"}
	w.RegisterActivityWithOptions(activities, activity.RegisterOptions{
		Name: "Activities",
	})
// ...
```
