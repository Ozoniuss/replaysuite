package replaytests

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// I can't seem to be triggering an NDE for this even with the dev server.
// tests. I tried a few combinations (change input type from string to int with
// an incompatible value, add more inputs) but it still doesn't seem to lead to
// an NDE.

func WorkflowWithInputChangeActivity(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with activity input started")

	// only use this when debugging with real server
	workflow.Sleep(ctx, 10*time.Second)

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var result string
	// Generate histories with a string input, then change this argument to 7
	// and update ActivityInputActivity to accept an int. Replay should not
	// fail with an NDE for the activity input change.
	// if err := workflow.ExecuteActivity(activityCtx, InputChangeActivity, "number 7").Get(ctx, &result); err != nil {
	// 	return "", err
	// }
	if err := workflow.ExecuteActivity(activityCtx, InputChangeActivity, 7).Get(ctx, &result); err != nil {
		return "", err
	}

	logger.Info("Workflow with activity input completed", "result", result)
	return result, nil
}

// func InputChangeActivity(ctx context.Context, value string) (string, error) {
// 	activity.GetLogger(ctx).Info("ActivityInputActivity", "value", value)
// 	value = value + "1"
// 	return "value:" + value, nil
// }

// Change ActivityInputActivity to this signature when switching the workflow
// ExecuteActivity argument from "7" to 7.

func InputChangeActivity(ctx context.Context, value int) (string, error) {
	activity.GetLogger(ctx).Info("ActivityInputActivity", "value", value)
	value = value + 1
	return fmt.Sprintf("value:%d", value), nil
}

func WorkflowWithAddedInputs(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with activity input started")

	// only use this when debugging with real server
	// workflow.Sleep(ctx, 10*time.Second)

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var result string
	// Generate histories with a string input, then change this argument to 7
	// and update ActivityInputActivity to accept an int. Replay should not
	// fail with an NDE for the activity input change.
	if err := workflow.ExecuteActivity(activityCtx, AddedInputsActivity, "number 6", 7).Get(ctx, &result); err != nil {
		return "", err
	}
	// if err := workflow.ExecuteActivity(activityCtx, AddedInputsActivity, 7).Get(ctx, &result); err != nil {
	// 	return "", err
	// }

	logger.Info("Workflow with activity input completed", "result", result)
	return result, nil
}

func AddedInputsActivity(ctx context.Context, name string, value int) (string, error) {
	activity.GetLogger(ctx).Info("ActivityInputActivity", "value", value)
	value = value + 1
	return fmt.Sprintf("value:%d", value), nil
}

// func AddedInputsActivity(ctx context.Context, value int) (string, error) {
// 	activity.GetLogger(ctx).Info("ActivityInputActivity", "value", value)
// 	value = value + 1
// 	return fmt.Sprintf("value:%d", value), nil
// }
