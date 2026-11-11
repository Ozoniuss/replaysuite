package replaysuite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// WorkflowTestSuite is a wrapper around temporal's WorkflowTestSuite. Because
// TestWorkflowEnvironment is constructed via WorkflowTestSuite, you must use
// the wrapper instead of the regular testsuite.WorkflowTestSuite in order to
// start the replay test environment and generate histories.
//
// Most of the times, this should be a drop-in replacement, since the test
// environment behavior can be controlled via environment variables. However,
// WorkflowTestSuite also exposes some additional methods if you want to
// explicitly control the behavior of the test suite.
//
// It is safe to run multiple workflow test suites against the same temporal
// server, assuming it is not already active and has other executions from
// different sources. When running against an active server, make sure there
// are no conflicting registrations int the same namespace and task queue.

// Make sure the namespace exists when running against an external temporal
// server.
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
		historiesDir: filepath.Join(os.Getenv("REPLAY_SUITE_HISTORIES_DIR"), defaultHistoriesDir),
	}
}

// Start will spin up a temporal server managed by the test process.
//
// It is recommended to run the tests against a local temporal server managed
// externally to the test suite. Otherwise, every workflow test suite will have
// to spin up and tear down a temporal server installation.
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

// Stop tears down the temporal server started via Start().
func (s *WorkflowTestSuite) Stop() error {
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		_ = s.server.Stop()
	}
	return nil
}
