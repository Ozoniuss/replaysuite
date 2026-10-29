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
			if err := workflow.ExecuteActivity(ctx, GoesToBranch1Activity).Get(ctx, nil); err != nil {
				logger.Error("GoesToBranch1Activity failed.", "Error", err)
				return BranchResult{}, err
			}
			b1cnt++
		} else {
			if err := workflow.ExecuteActivity(ctx, GoesToBranch2Activity).Get(ctx, nil); err != nil {
				logger.Error("GoesToBranch2Activity failed.", "Error", err)
				return BranchResult{}, err
			}
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

func WorkflowWithStruct(ctx workflow.Context, name string) (BranchResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started", "name", name)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	b1cnt := 0
	b2cnt := 0

	var a *Activities

	for range 10 {
		var echoName string
		if err := workflow.ExecuteActivity(ctx, a.EchoName, name).Get(ctx, &echoName); err != nil {
			logger.Error("GreetActivity failed.", "Error", err)
			return BranchResult{}, err
		}

		if echoName == Branch1Word {
			if err := workflow.ExecuteActivity(ctx, a.GoesToBranch1Activity).Get(ctx, nil); err != nil {
				logger.Error("GoesToBranch1Activity failed.", "Error", err)
				return BranchResult{}, err
			}
			b1cnt++
		} else {
			if err := workflow.ExecuteActivity(ctx, a.GoesToBranch2Activity).Get(ctx, nil); err != nil {
				logger.Error("GoesToBranch2Activity failed.", "Error", err)
				return BranchResult{}, err
			}
			b2cnt++
		}
	}

	return BranchResult{
		B1cnt: b1cnt,
		B2cnt: b2cnt,
	}, nil
}

type Activities struct {
}

func (a *Activities) GoesToBranch1Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 1")
	return nil
}

func (a *Activities) GoesToBranch2Activity(ctx context.Context) error {
	activity.GetLogger(ctx).Info("branch 2")
	return nil
}

func (a *Activities) EchoName(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("EchoName", "name", name)
	return name, nil
}
