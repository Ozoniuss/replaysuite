package replaysuite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
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

	config  config
	started bool
}

// TODO: this is not currently working at all
func (s *WorkflowTestSuite) NewTestWorkflowEnvironment() *TestWorkflowEnvironment {
	return &TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.WorkflowTestSuite.NewTestWorkflowEnvironment(),
		// hack: currently Start() must be called before NewTestWorkflowEnvironment
		tclient:    s.client,
		actSignRef: map[string]any{},
	}
}

type config struct {
	hostPort             string
	redactWorkerIdentity bool
	skipReplayTest       bool
	// replayWorkflows maps workflow type name to workflow function. Workflows
	// listed here are replayed against existing histories at Start and have
	// their mirrored executions dumped at Stop.
	replayWorkflows map[string]any
}

type Option func(*config)

// WithRedactWorkerIdentity replaces the worker identity and sticky task queue
// name in dumped histories with a generic placeholder.
func WithRedactWorkerIdentity() Option {
	return func(c *config) { c.redactWorkerIdentity = true }
}

// WithSkipReplayTest skips replaying existing histories against the current
// workflow implementations. New histories are still generated in the target
// location.
func WithSkipReplayTest() Option {
	return func(c *config) { c.skipReplayTest = true }
}

// WithReplayWorkflows registers workflow functions for replay testing.
// Only workflows registered here are replayed and dumped.
func WithReplayWorkflows(fns ...any) Option {
	return func(c *config) {
		if c.replayWorkflows == nil {
			c.replayWorkflows = make(map[string]any)
		}
		for _, fn := range fns {
			name, _ := getFunctionName(fn)
			c.replayWorkflows[name] = fn
		}
	}
}

// Start replays existing histories (unless WithSkipReplayTest is set), then
// starts the dev server.
func (s *WorkflowTestSuite) Start(opts ...Option) error {
	if s.started {
		return nil
	}
	for _, opt := range opts {
		opt(&s.config)
	}

	if !s.config.skipReplayTest {
		for workflowType, workflowFn := range s.config.replayWorkflows {
			if err := s.replayAndDeleteHistories(workflowType, workflowFn); err != nil {
				return fmt.Errorf("replay histories for workflow %s: %w", workflowType, err)
			}
		}
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

// Stop dumps observed histories and tears down the dev server.
func (s *WorkflowTestSuite) Stop() error {
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		_ = s.server.Stop()
	}
	return nil
}

func (s *WorkflowTestSuite) replayAndDeleteHistories(
	workflowType string,
	workflowFn any,
) error {

	dir := filepath.Join(defaultHistoriesDir, sanitize(workflowType))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read histories for workflow %s: %w", workflowType, err)
	}

	var historyFiles []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		historyFiles = append(historyFiles, filepath.Join(dir, entry.Name()))
	}
	if len(historyFiles) == 0 {
		return nil
	}
	sort.Strings(historyFiles)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(workflowFn)

	for _, historyFile := range historyFiles {
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, historyFile); err != nil {
			return fmt.Errorf("replay %s: %w", historyFile, err)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete replayed histories: %w", err)
	}
	return nil
}
