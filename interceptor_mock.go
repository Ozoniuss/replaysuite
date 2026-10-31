package replaysuite

import (
	"context"
	"fmt"
	"reflect"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// mockInterceptor returns the mocked result for activities and workflows that
// have an OnActivity or OnWorkflow expectation, and runs the registered
// implementation otherwise.
//
// The reason for this is to simplify handling activities that are registered
// to the test suite, but aren't mocked. Initially I used a dynamic handler to
// handle every activity and workflow execution, but that doesn't really make it
// easy to figure out exactly what needs to be run in case you have to run the
// real function. Instead, we can register every single actvitiy to the real
// worker and then we only need to handle the case when there are mocks defined.
//
// Additionally, it's no longer necessary to actually build the mock's arguments
// anymore, since they come directly from the interceptor function.
type mockInterceptor struct {
	interceptor.WorkerInterceptorBase
	actMock    *mock.Mock
	wfMock     *mock.Mock
	actSignRef map[string]any
	actMocked  map[string]struct{}
	wfMocked   map[string]struct{}
}

func (i *mockInterceptor) InterceptActivity(
	ctx context.Context, next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	in := &mockActivityInbound{mock: i.actMock, actSignRef: i.actSignRef, actMocked: i.actMocked}
	in.Next = next
	return in
}

func (i *mockInterceptor) InterceptWorkflow(
	ctx workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	in := &mockWorkflowInbound{mock: i.wfMock, wfMocked: i.wfMocked}
	in.Next = next
	return in
}

type mockWorkflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	mock     *mock.Mock
	wfMocked map[string]struct{}
}

func (in *mockWorkflowInbound) ExecuteWorkflow(
	ctx workflow.Context, input *interceptor.ExecuteWorkflowInput,
) (any, error) {

	name := workflow.GetInfo(ctx).WorkflowType.Name

	if _, ok := in.wfMocked[name]; ok {
		rets, err := methodCalled(in.mock, name, append([]any{ctx}, input.Args...))
		if err != nil {
			return nil, internalError(fmt.Errorf("replaysuite: workflow %s: %w", name, err))
		}
		return extractReturn(rets)
	}

	return in.Next.ExecuteWorkflow(ctx, input)
}

type mockActivityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	mock       *mock.Mock
	actSignRef map[string]any
	actMocked  map[string]struct{}
}

func (in *mockActivityInbound) ExecuteActivity(
	ctx context.Context, input *interceptor.ExecuteActivityInput,
) (any, error) {

	name := activity.GetInfo(ctx).ActivityType.Name

	if _, ok := in.actMocked[name]; ok {
		stubFn, ok := in.actSignRef[name]
		if !ok {
			return in.Next.ExecuteActivity(ctx, input)
		}

		fnType := reflect.TypeOf(stubFn)
		if fnType == nil || fnType.Kind() != reflect.Func {
			return nil, internalError(fmt.Errorf("replaysuite: no function signature for mocked activity %s", name))
		}
		args := input.Args
		if fnType.NumIn() > 0 && fnType.In(0).Implements(reflect.TypeFor[context.Context]()) {
			args = append([]any{ctx}, args...)
		}

		rets, err := methodCalled(in.mock, name, args)
		if err != nil {
			return nil, internalError(fmt.Errorf("replaysuite: activity %s: %w", name, err))
		}
		return extractReturn(rets)
	}

	return in.Next.ExecuteActivity(ctx, input)

}

func extractReturn(rets []any) (any, error) {
	switch len(rets) {
	case 0:
		return nil, nil
	case 1:
		if rets[0] == nil {
			return nil, nil
		}
		if e, ok := rets[0].(error); ok {
			return nil, e
		}
		return rets[0], nil
	default:
		var err error
		if rets[1] != nil {
			if e, ok := rets[1].(error); ok {
				err = e
			}
		}
		return rets[0], err
	}
}
