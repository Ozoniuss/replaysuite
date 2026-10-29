# replay-suite

Generate event histories for your workflow using your unit test suite, and use
them to write replay tests.

## Limitations

When activities are registered with a custom name, the Temporal SDK recommends
always referring to them by that name. For such activities, replaysuite only
matches the SDK test environment's behavior if the mocks also use the custom
name. The following actually fails the workflow because it can't find the
activity:

```go
type Activities struct {
}

func (a *Activities) GetGreeting() (string, error) {
	return "", nil
}


func GreetingSample(ctx workflow.Context) (string, error) {

    // call getGreeting via struct function
	err := workflow.ExecuteActivity(ctx, a.GetGreeting).Get(ctx, &greetResult)
	if err != nil {
		logger.Error("Get greeting failed.", "Error", err)
		return "", err
	}
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
