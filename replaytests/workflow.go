package replaytests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func Workflow(ctx workflow.Context, name string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started", "name", name)

	// Branch 1: input validation. An empty name takes the early-exit path.
	if name == "" {
		return "", errors.New("name must not be empty")
	}

	defaultCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	var greeting string
	if err := workflow.ExecuteActivity(defaultCtx, GreetActivity, name).Get(ctx, &greeting); err != nil {
		logger.Error("GreetActivity failed.", "Error", err)
		return "", err
	}

	// Uncomment this to fail the replay test.
	// if err := workflow.ExecuteActivity(defaultCtx, GreetActivity, name).Get(ctx, &greeting); err != nil {
	// 	logger.Error("GreetActivity failed.", "Error", err)
	// 	return "", err
	// }

	// Branch 2: pick a path based on the activity result, and run a
	// different activity on each path.
	var branch, branchResult string
	if len(greeting) > 12 {
		branch = "long"
		if err := workflow.ExecuteActivity(defaultCtx, LongPathActivity, greeting).Get(ctx, &branchResult); err != nil {
			logger.Error("LongPathActivity failed.", "Error", err)
			return "", err
		}
	} else {
		branch = "short"
		// The short branch runs a child workflow instead of an activity.
		if err := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow, greeting).Get(ctx, &branchResult); err != nil {
			logger.Error("ChildWorkflow failed.", "Error", err)
			return "", err
		}
	}
	logger.Info("took branch", "branch", branch, "branchResult", branchResult)

	// Two activities started in parallel, each with its own retry policy and
	// its own task queue.
	upperCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		TaskQueue:           "uppercase-queue",
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})
	reverseCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		TaskQueue:           "reverse-queue",
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})

	upperFuture := workflow.ExecuteActivity(upperCtx, UppercaseActivity, greeting)
	reverseFuture := workflow.ExecuteActivity(reverseCtx, ReverseActivity, greeting)

	var upper, reversed string
	if err := upperFuture.Get(ctx, &upper); err != nil {
		logger.Error("UppercaseActivity failed.", "Error", err)
		return "", err
	}
	if err := reverseFuture.Get(ctx, &reversed); err != nil {
		logger.Error("ReverseActivity failed.", "Error", err)
		return "", err
	}

	result := fmt.Sprintf("[%s:%s] %s | %s", branch, branchResult, upper, reversed)
	logger.Info("Workflow completed.", "result", result)
	return result, nil
}

// GreetActivity builds a greeting for the given name.
func GreetActivity(ctx context.Context, name string) (string, error) {
	activity.GetLogger(ctx).Info("GreetActivity", "name", name)
	return "Hello " + name + "!", nil
}

// LongPathActivity runs on the "long" branch and tags its input.
func LongPathActivity(ctx context.Context, s string) (string, error) {
	activity.GetLogger(ctx).Info("LongPathActivity", "input", s)
	return "long-handled(" + s + ")", nil
}

// UppercaseActivity returns its input upper-cased.
func UppercaseActivity(ctx context.Context, s string) (string, error) {
	activity.GetLogger(ctx).Info("UppercaseActivity", "input", s)
	return strings.ToUpper(s), nil
}

// ReverseActivity returns its input with the runes reversed.
func ReverseActivity(ctx context.Context, s string) (string, error) {
	activity.GetLogger(ctx).Info("ReverseActivity", "input", s)
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r), nil
}

// ChildWorkflow runs on the "short" branch and echoes its input.
func ChildWorkflow(ctx workflow.Context, name string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ChildWorkflow started", "name", name)

	return name, nil
}
