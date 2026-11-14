package replaysuite

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const (
	envTemporalServerHostport = "REPLAY_SUITE_TEMPORAL_SERVER_HOSTPORT"
	envHistoriesDir           = "REPLAY_SUITE_HISTORIES_DIR"
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

	// When this env is supplied, test workflow environment will create a client
	// connecting to that temporal server. This allows preserving existing calls
	// to NewTestWorkflowEnvironment() without any changes except the import
	// path.
	if hostport := os.Getenv(envTemporalServerHostport); hostport != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tc, err := client.DialContext(ctx, client.Options{
			HostPort: hostport,
		})
		if err != nil {
			panic(fmt.Sprintf("dial temporal client: %s", err.Error()))
		}
		s.client = tc
	}

	historiesDir := defaultHistoriesDir
	if hd := os.Getenv(envHistoriesDir); hd != "" {
		historiesDir = filepath.Join(hd, defaultHistoriesDir)
	}

	return &TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.WorkflowTestSuite.NewTestWorkflowEnvironment(),
		tclient:                 s.client,
		actSignRef:              map[string]any{},
		actMocked:               map[string]struct{}{},
		wfMocked:                map[string]struct{}{},
		actAliasRef:             map[string]string{},
		wfAliasRef:              map[string]string{},
		historiesDir:            historiesDir,
	}
}

// Start will spin up a temporal server managed by the test process.
//
// It is recommended to run the tests against a local temporal server managed
// externally to the test suite. Otherwise, every workflow test suite will have
// to spin up and tear down a temporal server installation.
//
// Note: calling Start() without calling Stop() will leave a temporal dev server
// process running. This can happen e.g. with unhandled panics, like the example
// below:
//
// func TestMain(m *testing.M) {
// 	if err := suite.Start(); err != nil {
// 		fmt.Fprintf(os.Stderr, "failed to start suite: %s", err.Error())
// 		os.Exit(1)
// 	}
// 	code := m.Run()
// 	_ = suite.Stop()
// 	os.Exit(code)
// }

//	func TestSampleWorkflow(t *testing.T) {
//	  // this test panics, without a recover
//	}
//
// The wrappers below should make the dev server crash on the first write with
// SIGPIPE which should happen fairly quickly.
func (s *WorkflowTestSuite) Start() error {

	// skip starting a test server when we want to connect to a real one
	if os.Getenv(envTemporalServerHostport) != "" {
		return nil
	}

	// In order to produce event histories, the replay test environment
	// spins up an in-process temporal cluster and a shared worker and
	// registers all workflows and activities on that shared worker.
	srv, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{
		// make the tests fail write away in case the test suite panics. this
		// wrapper will avoid leaving a running server when a test that panicked
		// did not call Stop().
		//
		// https://github.com/golang/go/blob/go1.26.6/src/os/exec/exec.go#L580-L610
		Stdout: struct{ io.Writer }{os.Stdout},
		Stderr: struct{ io.Writer }{os.Stderr},
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
