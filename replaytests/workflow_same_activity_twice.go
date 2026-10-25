package replaytests

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func WorkflowWithSameActivityCalledTwice(ctx workflow.Context, name string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started", "name", name)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	for range 2 {
		var greeting string
		if err := workflow.ExecuteActivity(ctx, GreetActivity, name).Get(ctx, &greeting); err != nil {
			logger.Error("GreetActivity failed.", "Error", err)
			return err
		}

		if greeting == "Hello Temporal!" {
			workflow.ExecuteActivity(ctx, GoesToBranch1Activity)
		} else {
			workflow.ExecuteActivity(ctx, GoesToBranch2Activity)
		}
	}

	return nil
}

func GoesToBranch1Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 1")
	return nil
}

func GoesToBranch2Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 1")
	return nil
}
