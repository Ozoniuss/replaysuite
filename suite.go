package replaysuite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultHistoriesDir       = ".histories"
	decodedPayloadDataJSONKey = "__replaysuite_decodedData"
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

func (s *WorkflowTestSuite) NewTestWorkflowEnvironment() *TestWorkflowEnvironment {
	return &TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.WorkflowTestSuite.NewTestWorkflowEnvironment(),
		// hack: currently Start() must be called before NewTestWorkflowEnvironment
		tclient:           s.client,
		activityStubFnRef: map[string]any{},
		workflowStubFnRef: map[string]any{},
		dumpHistory:       dumpSingleHistory,
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

func sanitize(s string) string { return strings.ReplaceAll(s, "/", "_") }

func dumpSingleHistory(wfType, wfId string, hist *historypb.History) error {

	// needed to overwrite history, e.g. redact identities
	marshal := protojson.MarshalOptions{Indent: "  "}
	raw, err := marshal.Marshal(hist)
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	// payloads are base64 encoded which makes it less convenient to
	// inspect the actual values in the history. This also records a
	// new field in the history with the decoded value. The replayer
	// discards unknown fields so it should be ok.
	//https://github.com/temporalio/sdk-go/blob/35367242edb9e92be90825cd7653ab0046a660d1/internal/internal_worker.go#L2054
	annotatePayloadData(value)
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	workflowDir := filepath.Join(defaultHistoriesDir, sanitize(wfType))
	if err := os.MkdirAll(workflowDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(workflowDir, wfId+".json"), out, 0o600); err != nil {
		return err
	}

	return nil

}

// redactWorkerIdentityInHistory makes the worker identity in the history
// anonymous. It finds all events that reference the worker either as an
// identity or a sticky task queue.
//
// Note that because multiple protos in the history have an identity field,
// this helper makes the change on the raw value rather than a history proto
// object.
func redactWorkerIdentityInHistory(value any) {
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v["identity"].(string); ok {
			v["identity"] = "placeholder"
		}
		if kind, ok := v["kind"].(string); ok && kind == "TASK_QUEUE_KIND_STICKY" {
			if name, ok := v["name"].(string); ok {
				if idx := strings.LastIndex(name, ":"); idx >= 0 {
					v["name"] = "placeholder" + name[idx:]
				} else {
					v["name"] = "placeholder"
				}
			}
		}
		for _, child := range v {
			redactWorkerIdentityInHistory(child)
		}
	case []any:
		for _, child := range v {
			redactWorkerIdentityInHistory(child)
		}
	}
}

func annotatePayloadData(value any) {
	switch v := value.(type) {
	case map[string]any:
		if decoded, ok := decodePayloadData(v); ok {
			v[decodedPayloadDataJSONKey] = decoded
		}
		for _, child := range v {
			annotatePayloadData(child)
		}
	case []any:
		for _, child := range v {
			annotatePayloadData(child)
		}
	}
}

func decodePayloadData(payload map[string]any) (any, bool) {
	metadata, ok := payload["metadata"].(map[string]any)
	if !ok {
		return nil, false
	}
	encodedEncoding, ok := metadata["encoding"].(string)
	if !ok {
		return nil, false
	}
	encodingBytes, err := base64.StdEncoding.DecodeString(encodedEncoding)
	if err != nil {
		return nil, false
	}

	switch string(encodingBytes) {
	case "binary/null":
		return nil, true
	case "json/plain", "json/protobuf":
		encodedData, ok := payload["data"].(string)
		if !ok {
			return nil, false
		}
		data, err := base64.StdEncoding.DecodeString(encodedData)
		if err != nil {
			return nil, false
		}
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			return nil, false
		}
		return decoded, true
	default:
		return nil, false
	}
}
