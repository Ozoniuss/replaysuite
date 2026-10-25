package replaysuite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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
		suite:                   s,
		activityStubs:           map[string][]stubCall{},
		activityStubFnRef:       map[string]any{},
		workflowStubs:           map[string][]stubCall{},
		workflowStubFnRef:       map[string]any{},
	}
}

type config struct {
	// TODO: add support for any type of FS.
	historiesDir         string
	redactWorkerIdentity bool
	skipReplayTest       bool
	// replayWorkflows maps workflow type name to workflow function. Workflows
	// listed here are replayed against existing histories at Start and have
	// their mirrored executions dumped at Stop.
	replayWorkflows map[string]interface{}
}

type Option func(*config)

// WithHistoriesDir overrides the directory used to read existing histories at
// Start and to write new histories at Stop. Defaults to ".histories".
func WithHistoriesDir(dir string) Option {
	return func(c *config) { c.historiesDir = dir }
}

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
func WithReplayWorkflows(fns ...interface{}) Option {
	return func(c *config) {
		if c.replayWorkflows == nil {
			c.replayWorkflows = make(map[string]interface{})
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
	if s.config.historiesDir == "" {
		s.config.historiesDir = defaultHistoriesDir
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
	if !s.started {
		return nil
	}
	var dumperr error
	if s.client != nil && s.config.historiesDir != "" {
		if err := dumpAllHistories(context.Background(), s.client, s.config.historiesDir, s.config.replayWorkflows, s.config.redactWorkerIdentity); err != nil {
			dumperr = err
		}
	}
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		_ = s.server.Stop()
	}
	s.started = false
	return dumperr
}

func (s *WorkflowTestSuite) replayAndDeleteHistories(
	workflowType string,
	workflowFn interface{},
) error {
	if s.config.historiesDir == "" || workflowType == "" {
		return nil
	}

	dir := filepath.Join(s.config.historiesDir, sanitize(workflowType))
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

// dynamicActivityHandler infers what to return for an activity based on
// mocked outputs.
func (env *TestWorkflowEnvironment) dynamicActivityHandler(ctx context.Context, payloads converter.EncodedValues) (interface{}, error) {
	info := activity.GetInfo(ctx)
	wfID := info.WorkflowExecution.ID
	actName := info.ActivityType.Name

	stubFn, ok := env.activityStubFnRef[actName]
	if !ok {
		return nil, fmt.Errorf("replaysuite: no stub for activity %s in workflow %s", actName, wfID)
	}

	decoded, err := decodePayloadsViaFnType(payloads, stubFn)
	if err != nil {
		return nil, fmt.Errorf("decode payloads for %s: %w", actName, err)
	}

	for _, c := range env.activityStubs[actName] {
		if stubCallMatches(c, decoded) {
			return extractReturn(c.returns)
		}
	}
	return nil, fmt.Errorf("replaysuite: no stub matched activity %s args=%v", actName, decoded)
}

// dynamicWorkflowHandler serves child workflows from the OnWorkflow mocks.
func (env *TestWorkflowEnvironment) dynamicWorkflowHandler(ctx workflow.Context, payloads converter.EncodedValues) (interface{}, error) {
	info := workflow.GetInfo(ctx)
	wfType := info.WorkflowType.Name

	parent := info.ParentWorkflowExecution
	if parent == nil {
		return nil, fmt.Errorf("replaysuite: workflow %s has no parent; only mocked child workflows are served by the dynamic handler", wfType)
	}

	stubFn, ok := env.workflowStubFnRef[wfType]
	if !ok {
		return nil, fmt.Errorf("replaysuite: no OnWorkflow stub for child workflow %s in workflow %s", wfType, parent.ID)
	}

	decoded, err := decodePayloadsViaFnType(payloads, stubFn)
	if err != nil {
		return nil, fmt.Errorf("decode payloads for child workflow %s: %w", wfType, err)
	}

	for _, c := range env.workflowStubs[wfType] {
		if stubCallMatches(c, decoded) {
			return extractReturn(c.returns)
		}
	}
	return nil, fmt.Errorf("replaysuite: no OnWorkflow stub matched child workflow %s args=%v", wfType, decoded)
}

func stubCallMatches(c stubCall, decoded []interface{}) bool {
	_, differences := mock.Arguments(c.expectedArgs).Diff(decoded)
	return differences == 0
}

// decodePayloadsViaFnType decodes raw payloads into Go values using the
// parameter types of the user-supplied function (skipping the leading
// context arg — context.Context for activities, workflow.Context for
// workflows). The decoded values are returned in the same order/shape as the
// args passed to OnActivity/OnWorkflow (after context-strip).
func decodePayloadsViaFnType(payloads converter.EncodedValues, fn interface{}) ([]interface{}, error) {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("not a function")
	}
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	wfCtxType := reflect.TypeOf((*workflow.Context)(nil)).Elem()

	startIdx := 0
	if fnType.NumIn() > 0 {
		if first := fnType.In(0); first.Implements(ctxType) || first.Implements(wfCtxType) {
			startIdx = 1
		}
	}
	argCount := fnType.NumIn() - startIdx
	ptrs := make([]interface{}, argCount)
	values := make([]reflect.Value, argCount)
	for i := 0; i < argCount; i++ {
		t := fnType.In(i + startIdx)
		v := reflect.New(t)
		ptrs[i] = v.Interface()
		values[i] = v
	}
	if argCount > 0 {
		if err := payloads.Get(ptrs...); err != nil {
			return nil, err
		}
	}
	out := make([]interface{}, argCount)
	for i, v := range values {
		out[i] = v.Elem().Interface()
	}
	return out, nil
}

// extractReturn converts the user's [result, err] or [err] slice into
// the dynamic activity's (interface{}, error) return shape. This
// effectively implements the activity's logic to simply return what
// was specified by the mock, which is good enough since the event
// history is independent of the activity's logic.
func extractReturn(rets []interface{}) (interface{}, error) {
	switch len(rets) {
	case 0:
		return nil, nil
	case 1:
		if rets[0] == nil {
			return nil, nil
		}
		if e, ok := rets[0].(error); ok {
			return nil, e
		}
		return rets[0], nil
	default:
		var err error
		if rets[1] != nil {
			if e, ok := rets[1].(error); ok {
				err = e
			}
		}
		return rets[0], err
	}
}

func sanitize(s string) string { return strings.ReplaceAll(s, "/", "_") }

func historyFileName(workflowID, runID string) string {
	if runID == "" {
		return sanitize(workflowID) + ".json"
	}
	return sanitize(workflowID) + "_" + sanitize(runID) + ".json"
}

// dumpAllHistories writes workflow histories visible to c as JSON into
// dir/<sanitized workflow type>/<sanitized workflowID>_<sanitized runID>.json.
//
// Only executions whose workflow type is in replayTypes (the set registered
// via RegisterWorkflowForReplay) are dumped. Everything else on the dev
// server — most notably the stub child-workflow executions served by the
// dynamic workflow handler — is skipped: those histories are not
// representative of the real workflow implementation and are never replayed,
// so writing them would only leave uncleaned files in dir.
func dumpAllHistories(ctx context.Context, c client.Client, dir string, replayTypes map[string]interface{}, redactWorkerIdentity bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var nextPageToken []byte
	marshal := protojson.MarshalOptions{Indent: "  "}

	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return err
		}
		for _, exec := range resp.Executions {
			wfID := exec.Execution.GetWorkflowId()
			runID := exec.Execution.GetRunId()
			workflowType := exec.GetType().GetName()
			if _, ok := replayTypes[workflowType]; !ok {
				continue
			}
			// generate history events
			hist := &historypb.History{}
			iter := c.GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for iter.HasNext() {
				ev, err := iter.Next()
				if err != nil {
					return err
				}
				hist.Events = append(hist.Events, ev)
			}

			// convert from pb to any since that makes applying certain history
			// overrides much easier, see how below functions are implemented.
			raw, err := marshal.Marshal(hist)
			if err != nil {
				return err
			}
			var value interface{}
			if err := json.Unmarshal(raw, &value); err != nil {
				return err
			}
			// payloads are base64 encoded which makes it less convenient to
			// inspect the actual values in the history. This also records a
			// new field in the history with the decoded value. The replayer
			// discards unknown fields so it should be ok.
			//https://github.com/temporalio/sdk-go/blob/35367242edb9e92be90825cd7653ab0046a660d1/internal/internal_worker.go#L2054
			annotatePayloadData(value)
			if redactWorkerIdentity {
				redactWorkerIdentityInHistory(value)
			}
			out, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				return err
			}
			workflowDir := filepath.Join(dir, sanitize(workflowType))
			if err := os.MkdirAll(workflowDir, 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(workflowDir, historyFileName(wfID, runID)), out, 0o600); err != nil {
				return err
			}
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			return nil
		}
	}
}

// redactWorkerIdentityInHistory makes the worker identity in the history
// anonymous. It finds all events that reference the worker either as an
// identity or a sticky task queue.
//
// Note that because multiple protos in the history have an identity field,
// this helper makes the change on the raw value rather than a history proto
// object.
func redactWorkerIdentityInHistory(value interface{}) {
	switch v := value.(type) {
	case map[string]interface{}:
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
	case []interface{}:
		for _, child := range v {
			redactWorkerIdentityInHistory(child)
		}
	}
}

func annotatePayloadData(value interface{}) {
	switch v := value.(type) {
	case map[string]interface{}:
		if decoded, ok := decodePayloadData(v); ok {
			v[decodedPayloadDataJSONKey] = decoded
		}
		for _, child := range v {
			annotatePayloadData(child)
		}
	case []interface{}:
		for _, child := range v {
			annotatePayloadData(child)
		}
	}
}

func decodePayloadData(payload map[string]interface{}) (interface{}, bool) {
	metadata, ok := payload["metadata"].(map[string]interface{})
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
		var decoded interface{}
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
