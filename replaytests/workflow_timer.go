package replaytests

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Workflow is to demo how to setup query handler
func WorkflowWithTimer(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with timer started")

	// Wait for the timer to fire. Change the value of the timer to 0 to create
	// an NDE with the Go SDK.
	_ = workflow.NewTimer(ctx, time.Second*1).Get(ctx, nil)
	// _ = workflow.NewTimer(ctx, time.Second*0).Get(ctx, nil)
	logger.Info("Timer fired")

	logger.Info("QueryWorkflow completed")
	return nil
}
