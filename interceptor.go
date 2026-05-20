package replaysuite

import (
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// shortRetryInterval is the value we clamp retry intervals to.
// Anything > 0 is fine since changing retry options does not lead to
// nondeterminism.
const shortRetryInterval = time.Millisecond

// fastReplayInterceptor is a worker interceptor intended for the
// shared replay worker that rewrites every workflow's ExecuteActivity,
// NewTimer and Sleep call to be effectively instant.
type fastReplayInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (i *fastReplayInterceptor) InterceptWorkflow(
	ctx workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &fastReplayInbound{root: next}
}

type fastReplayInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root interceptor.WorkflowInboundInterceptor
}

func (in *fastReplayInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	wrapped := &fastReplayOutbound{}
	wrapped.Next = outbound
	in.Next = in.root
	return in.root.Init(wrapped)
}

type fastReplayOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (o *fastReplayOutbound) ExecuteActivity(
	ctx workflow.Context, activityType string, args ...interface{},
) workflow.Future {
	opts := workflow.GetActivityOptions(ctx)
	opts.TaskQueue = sharedTaskQueue
	// This is intended. Testing an activity with an infinite retry
	// policy and a forced failure on that activity is probably a
	// wrong test.
	if opts.RetryPolicy == nil {
		opts.RetryPolicy = &temporal.RetryPolicy{}
	}
	opts.RetryPolicy.InitialInterval = shortRetryInterval
	opts.RetryPolicy.MaximumInterval = shortRetryInterval
	opts.RetryPolicy.BackoffCoefficient = 1.0
	ctx = workflow.WithActivityOptions(ctx, opts)
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *fastReplayOutbound) NewTimer(ctx workflow.Context, d time.Duration) workflow.Future {
	return o.Next.NewTimer(ctx, shortRetryInterval)
}

func (o *fastReplayOutbound) Sleep(ctx workflow.Context, d time.Duration) error {
	return o.Next.Sleep(ctx, shortRetryInterval)
}
