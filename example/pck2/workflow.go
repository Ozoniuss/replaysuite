package pck2

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

func ActivityPck2(ctx context.Context) error {
	return nil
}

func SampleWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
	})
	return workflow.ExecuteActivity(ctx, ActivityPck2).Get(ctx, nil)
}
