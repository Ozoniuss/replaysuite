package replaysuite

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// WorkflowTestSuite is a drop-in replacement for testsuite.WorkflowTestSuite.
// In addition to the normal in-process  unit-test execution, each workflow runs
// against a real dev which produces event histories that can be used for replay
// testing. Existing tests written against *testsuite.TestWorkflowEnvironment keep
// working as-is.
type WorkflowTestSuite struct {
	testsuite.WorkflowTestSuite

	server *testsuite.DevServer
	client client.Client

	started bool
}

func (s *WorkflowTestSuite) NewTestWorkflowEnvironment() *TestWorkflowEnvironment {
	return &TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.WorkflowTestSuite.NewTestWorkflowEnvironment(),
		// hack: currently Start() must be called before NewTestWorkflowEnvironment
		tclient:      s.client,
		actSignRef:   map[string]any{},
		actMocked:    map[string]struct{}{},
		wfMocked:     map[string]struct{}{},
		actAliasRef:  map[string]string{},
		wfAliasRef:   map[string]string{},
		hw:           NewDefaultHistoryWriter(),
		he:           NewDefaultHistoryEncoder(),
		historiesDir: defaultHistoriesDir,
	}
}

// Start starts the dev server.
func (s *WorkflowTestSuite) Start() error {
	if s.started {
		return nil
	}

	// In order to produce event histories, the replay test environment
	// spins up an in-process temporal cluster and a shared worker and
	// registers all workflows and activities on that shared worker.
	srv, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{
		// Without this option, the minimum duration of a timer or a retry seems
		// to be capped at 1s.
		ExtraArgs: []string{
			"--dynamic-config-value", `history.timerProcessorMaxTimeShift="1ms"`,
		},
	})
	if err != nil {
		return fmt.Errorf("start dev server: %w", err)
	}
	s.server = srv
	s.client = srv.Client()
	s.started = true
	return nil
}

// Stop tears down the dev server.
func (s *WorkflowTestSuite) Stop() error {
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		_ = s.server.Stop()
	}
	return nil
}
