package testworkflowkey

import "go.temporal.io/sdk/workflow"

func Workflow(ctx workflow.Context) error { return nil }

var WorkflowVar = func(ctx workflow.Context) error { return nil }
