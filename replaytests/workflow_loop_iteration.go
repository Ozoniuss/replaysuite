package replaytests

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func WorkflowWithLoopIterationChange(ctx workflow.Context) ([]string, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	iterationCount := 2
	// Generate histories with 2 iterations, then change this to 3 to trigger a
	// replay failure.
	// iterationCount := 3

	results := make([]string, 0, iterationCount)
	for i := 0; i < iterationCount; i++ {
		var result string
		if err := workflow.ExecuteActivity(activityCtx, LoopIterationActivity, i).Get(ctx, &result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

func LoopIterationActivity(ctx context.Context, iteration int) (string, error) {
	activity.GetLogger(ctx).Info("LoopIterationActivity", "iteration", iteration)
	return fmt.Sprintf("iteration-%d", iteration), nil
}
