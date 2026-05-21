package replaytests

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func WorkflowWithTimer(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with timer started")

	// Wait for the timer to fire. Change the value of the timer to 0 to create
	// an NDE with the Go SDK.
	_ = workflow.NewTimer(ctx, time.Second*1).Get(ctx, nil)
	// _ = workflow.NewTimer(ctx, time.Second*0).Get(ctx, nil)
	logger.Info("Timer fired")

	logger.Info("WorkflowWithTimer completed")
	return nil
}

func WorkflowWithZeroTimer(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with timer started")

	// Wait for the timer to fire. Change the value of the timer to something
	// positive to get an NDE.
	_ = workflow.NewTimer(ctx, time.Second*0).Get(ctx, nil)
	// _ = workflow.NewTimer(ctx, time.Second*1).Get(ctx, nil)
	logger.Info("Timer fired")

	logger.Info("WorkflowWithZeroTimer completed")
	return nil
}
