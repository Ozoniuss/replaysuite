package replaytests

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// This is not actually part of a replay test, but does allow testing that the
// interceptor implementation works as expected and that it does not hang
// waiting for this retry policy to be consumed.
func WorkflowWithActivityCustomRetryPolicy(ctx workflow.Context) (string, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        1 * time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        100 * time.Second,
			MaximumAttempts:        50,
			NonRetryableErrorTypes: []string{},
		},
	})

	var result string
	// this should execute instantly on the replay suite.
	if err := workflow.ExecuteActivity(activityCtx, ActivityWithCustomRetryPolicy, "Temporal").Get(ctx, &result); err != nil {
		return "", err
	}

	return result, nil
}

func ActivityWithCustomRetryPolicy(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("ActivityWithCustomRetryPolicy", "name", name)
	return fmt.Sprintf("hello %s", name), nil
}
