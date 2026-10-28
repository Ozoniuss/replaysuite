package regulartests

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const Branch1Word = "Temporal"

type BranchResult struct {
	B1cnt int
	B2cnt int
}

func Workflow(ctx workflow.Context, name string) (BranchResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started", "name", name)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	b1cnt := 0
	b2cnt := 0

	for range 10 {
		var echoName string
		if err := workflow.ExecuteActivity(ctx, EchoName, name).Get(ctx, &echoName); err != nil {
			logger.Error("GreetActivity failed.", "Error", err)
			return BranchResult{}, err
		}

		if echoName == Branch1Word {
			workflow.ExecuteActivity(ctx, GoesToBranch1Activity)
			b1cnt++
		} else {
			workflow.ExecuteActivity(ctx, GoesToBranch2Activity)
			b2cnt++
		}
	}

	return BranchResult{
		B1cnt: b1cnt,
		B2cnt: b2cnt,
	}, nil
}

func GoesToBranch1Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 1")
	return nil
}

func GoesToBranch2Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 2")
	return nil
}

func EchoName(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("EchoName", "name", name)
	return name, nil
}
