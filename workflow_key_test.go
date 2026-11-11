package replaysuite

import (
	"testing"

	"go.temporal.io/sdk/workflow"

	"github.com/Ozoniuss/replaysuite/internal/testworkflowkey"
)

func keyTestWorkflow(ctx workflow.Context) error { return nil }

var keyTestWorkflowVar = func(ctx workflow.Context) error { return nil }

func TestWorkflowKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   any
		want string
	}{
		{
			name: "function from current package",
			fn:   keyTestWorkflow,
			want: "github.com_Ozoniuss_replaysuite.keyTestWorkflow",
		},
		{
			name: "function from imported package",
			fn:   testworkflowkey.Workflow,
			want: "github.com_Ozoniuss_replaysuite_internal_testworkflowkey.Workflow",
		},
		{
			name: "variable from current package",
			fn:   keyTestWorkflowVar,
			want: "github.com_Ozoniuss_replaysuite.init.func1",
		},
		{
			name: "variable from imported package",
			fn:   testworkflowkey.WorkflowVar,
			want: "github.com_Ozoniuss_replaysuite_internal_testworkflowkey.init.func1",
		},
		{
			name: "anonymous function",
			fn:   func(ctx workflow.Context) error { return nil },
			want: "github.com_Ozoniuss_replaysuite.TestWorkflowKey.func1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := workflowKey(tt.fn); got != tt.want {
				t.Errorf("workflowKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
