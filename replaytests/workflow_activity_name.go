package replaytests

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func WorkflowWithActivityNameChange(ctx workflow.Context) (string, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var result string
	// Generate histories with OriginalNameActivity, then switch this call to
	// RenamedActivity below to trigger a replay failure.
	if err := workflow.ExecuteActivity(activityCtx, OriginalNameActivity, "Temporal").Get(ctx, &result); err != nil {
		return "", err
	}
	// if err := workflow.ExecuteActivity(activityCtx, RenamedActivity, "Temporal").Get(ctx, &result); err != nil {
	// 	return "", err
	// }

	return result, nil
}

func OriginalNameActivity(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("OriginalNameActivity", "name", name)
	return fmt.Sprintf("hello %s", name), nil
}

func RenamedActivity(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("RenamedActivity", "name", name)
	return fmt.Sprintf("hello %s", name), nil
}
