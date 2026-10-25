package replaysuite

import (
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const shortRetryInterval = time.Millisecond

// fastReplayInterceptor tries to make all worker calls that are running in the
// replay test suite happen instantaneously.
type fastReplayInterceptor struct {
	interceptor.WorkerInterceptorBase
	taskQueue string
}

func (i *fastReplayInterceptor) InterceptWorkflow(
	ctx workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &fastReplayInbound{root: next, taskQueue: i.taskQueue}
}

type fastReplayInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root      interceptor.WorkflowInboundInterceptor
	taskQueue string
}

func (in *fastReplayInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	wrapped := &fastReplayOutbound{taskQueue: in.taskQueue}
	wrapped.Next = outbound
	in.Next = in.root
	return in.root.Init(wrapped)
}

type fastReplayOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	taskQueue string
}

func (o *fastReplayOutbound) ExecuteActivity(
	ctx workflow.Context, activityType string, args ...interface{},
) workflow.Future {
	opts := workflow.GetActivityOptions(ctx)
	// Overwrite the task queue to the one used by the replay worker.
	opts.TaskQueue = o.taskQueue
	// Overwrite the retry policy to something that executes quickly on the
	// dev server.
	if opts.RetryPolicy == nil {
		opts.RetryPolicy = &temporal.RetryPolicy{}
	}
	opts.RetryPolicy.InitialInterval = shortRetryInterval
	opts.RetryPolicy.MaximumInterval = shortRetryInterval
	opts.RetryPolicy.BackoffCoefficient = 1.0
	// It currently seems that even though the retry interval is below 1 second,
	// the activity retries are still attempted once every second on the dev
	// server. Disabling retries until I figure out if this is actually a
	// limitation.
	//
	// TODO: this overwrites infinite retries. need to figure out exactly what
	// to do there.
	opts.RetryPolicy.MaximumAttempts = 1
	ctx = workflow.WithActivityOptions(ctx, opts)
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *fastReplayOutbound) NewTimer(ctx workflow.Context, d time.Duration) workflow.Future {
	// avoid recording an event if sending a non-positive duration, like the
	// sdk does
	if d <= 0 {
		return o.Next.NewTimer(ctx, d)
	}
	return o.Next.NewTimer(ctx, shortRetryInterval)
}

func (o *fastReplayOutbound) Sleep(ctx workflow.Context, d time.Duration) error {
	// avoid recording an event if sending a non-positive duration, like the
	// sdk does
	if d <= 0 {
		return o.Next.Sleep(ctx, d)
	}
	return o.Next.Sleep(ctx, shortRetryInterval)
}
