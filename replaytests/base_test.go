package replaytests

import (
	"github.com/Ozoniuss/replaysuite"
	"github.com/stretchr/testify/suite"
)

// Use BaseTestSuite in other workflow test suites to only spin up the temporal
// server once.
type BaseTestSuite struct {
	suite.Suite
	// Import replay suite as a replacement for the test workflow suite. This
	// will run the regular unit tests, but on top of that it will also emit
	// event histories for each test, as well as do replay testing for each
	// workflow that is registered for that.
	replaysuite.WorkflowTestSuite
	env *replaysuite.Env
}

func (s *BaseTestSuite) TearDownSuite() {
	s.Require().NoError(s.WorkflowTestSuite.Stop())
}
