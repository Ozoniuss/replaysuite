package replaytests

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func WorkflowWithSleep(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with sleep started")

	// Change the value of the sleep to 0 to get an NDE.
	workflow.Sleep(ctx, 1*time.Second)
	// workflow.Sleep(ctx, 0*time.Second)

	logger.Info("Workflow with sleep completed")
	return nil
}

func WorkflowWithZeroSleep(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow with zero sleep started")

	// Change the value of the sleep to something positive to get an NDE.
	workflow.Sleep(ctx, 0*time.Second)
	// workflow.Sleep(ctx, 1*time.Second)

	logger.Info("Workflow with zero sleep completed")
	return nil
}
