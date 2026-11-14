package example

import (
	"bytes"
	"testing"

	"github.com/Ozoniuss/replaysuite"
	internalpck1 "github.com/Ozoniuss/replaysuite/example/internal/pck1"
	"github.com/Ozoniuss/replaysuite/example/nameddir"
	"github.com/Ozoniuss/replaysuite/example/pck1"
	"github.com/Ozoniuss/replaysuite/example/pck2"
)

func TestReplay(t *testing.T) {
	var suite replaysuite.ReplaySuite

	suite.Register(pck1.SampleWorkflow)
	suite.Register(pck2.SampleWorkflow)
	suite.Register(internalpck1.SampleWorkflow)
	suite.RegisterWithOptions(nameddir.SampleWorkflow, replaysuite.RegisterOptions{
		WorkflowFolder: "named_directory",
	})

	var archive bytes.Buffer
	if err := replaysuite.WriteArchiveFromHistories(&archive, ".histories"); err != nil {
		t.Fatal(err)
	}
	if err := suite.Replay(&archive); err != nil {
		t.Fatal(err)
	}
}
