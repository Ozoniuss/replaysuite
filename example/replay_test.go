package example

import (
	"testing"

	"github.com/Ozoniuss/replaysuite"
	internalpck1 "github.com/Ozoniuss/replaysuite/example/internal/pck1"
	"github.com/Ozoniuss/replaysuite/example/pck1"
	"github.com/Ozoniuss/replaysuite/example/pck2"
)

func TestReplay(t *testing.T) {
	var suite replaysuite.ReplaySuite

	suite.Register(pck1.SampleWorkflow)
	suite.Register(pck2.SampleWorkflow)
	suite.Register(internalpck1.SampleWorkflow)

	if err := suite.Replay(); err != nil {
		t.Fatal(err)
	}
}
