package ignored

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

func ActivityIgnored(ctx context.Context) error {
	return nil
}

func SampleWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
	})
	return workflow.ExecuteActivity(ctx, ActivityIgnored).Get(ctx, nil)
}
