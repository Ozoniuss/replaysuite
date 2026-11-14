package nameddir

import (
	"fmt"
	"os"
	"testing"

	"github.com/Ozoniuss/replaysuite"
)

const workflowFolder = "named_directory"

var suite replaysuite.WorkflowTestSuite

func TestMain(m *testing.M) {
	if err := suite.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start suite: %s", err.Error())
		os.Exit(1)
	}
	code := m.Run()
	_ = suite.Stop()
	os.Exit(code)
}

func TestSampleWorkflow(t *testing.T) {
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkflowFolder(workflowFolder)
	env.RegisterActivity(ActivityNamedDir)

	env.ExecuteWorkflow(SampleWorkflow)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
}
