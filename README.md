# replay-suite

Generate event histories for your workflow using your unit test suite, and use
them to write replay tests.

## Limitations

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
