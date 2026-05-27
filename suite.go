// Package replaysuite provides a drop-in wrapper around
// testsuite.WorkflowTestSuite that, in addition to the normal in-process
// unit-test execution, mirrors each workflow run against a real dev
// server so that real event histories are produced for replay testing.
//
// A workflow-outbound interceptor on the mirror worker clamps all
// retry-policy intervals and timer durations to 1ms, and forces every
// activity onto a single shared task queue. Activity options aren't
// checked for non-determinism on replay, so rewriting them is safe and
// removes the wall-clock cost of retry backoff and workflow timers.
//
// Existing tests written against *testsuite.TestWorkflowEnvironment keep
// working as-is — the wrapper delegates every call to a real test
// environment, so virtual time, mocked activities and child workflows,
// assertions, signals, and result/error inspection behave exactly as
// before. The dev-server mirror is purely additive.
//
// Per-test semantics are preserved: each test still creates a fresh
// *testsuite.TestWorkflowEnvironment via NewDevServerEnvironment. The
// expensive bits — dev server, worker, dynamic activity handler — are
// shared at the suite level so the per-test overhead stays small.
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
	"sync"
	"sync/atomic"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultHistoriesDir       = ".histories"
	sharedTaskQueue           = "replaysuite-shared"
	decodedPayloadDataJSONKey = "__replaysuite_decodedData"
)

// Suite is a drop-in replacement for embedding testsuite.WorkflowTestSuite
// in a testify suite. It additionally owns a dev server (one per suite
// run) and dumps every workflow history it observes to HistoriesDir on
// tear-down.
type Suite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	server   *testsuite.DevServer
	client   client.Client
	envCount int64

	// Shared worker, created lazily when workflows are registered and started
	// lazily on first ExecuteWorkflow. All Envs share it so the per-test
	// worker startup cost is paid once.
	workerCreateOnce sync.Once
	workerStartOnce  sync.Once
	worker           worker.Worker
	workerCreateErr  error
	workerStartErr   error
	envsByWFID       sync.Map // workflowID -> *Env, consulted by the dynamic activity handler
	workflowSet      sync.Map // workflow type name -> struct{}, dedupes RegisterWorkflow calls

	// Workflows to replay, key being the workflow type string and value being
	// the workflow function.
	workflowsTypesToReplayTest map[string]interface{}

	wfIDsMu sync.Mutex
	wfIDs   []string // every workflowID launched on the mirror; consulted by dumpAllHistories

	// options bundles user-configurable knobs. See SuiteOptions and
	// SetOptions. Must be set before SetupSuite runs.
	options SuiteOptions
}

// SuiteOptions holds the configurable options for the replay suite. See each
// option for more details.
type SuiteOptions struct {
	// HistoriesDir is the directory that dumped JSON histories are written
	// to on tear-down (and read from on SetupSuite for replay). If empty,
	// defaults to ".histories".
	HistoriesDir string

	// RedactWorkerIdentity, when true, causes dumped histories to have the
	// worker identity and sticky task queue name replaced with a generic
	// placeholder. Useful when histories are committed to source control and
	// the raw values (which embed the host name and a random per-run suffix)
	// would otherwise produce noisy diffs across runs. Defaults to false.
	RedactWorkerIdentity bool
}

// SetOptions configures the suite. Must be called before SetupSuite runs.
func (s *Suite) SetOptions(opts SuiteOptions) {
	s.options = opts
}

func (s *Suite) SetupSuite() {
	if s.options.HistoriesDir == "" {
		s.options.HistoriesDir = defaultHistoriesDir
	}
	for workflowType, workflowFn := range s.workflowsTypesToReplayTest {
		if err := s.replayAndDeleteHistories(workflowType, workflowFn); err != nil {
			s.Require().NoErrorf(err, "replay histories for workflow %s", workflowType)
		}
	}
	srv, err := testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{
		// Without this option, the minimum duration of a timer or a retry seems
		// to be capped at 1s.
		ExtraArgs: []string{
			"--dynamic-config-value", `history.timerProcessorMaxTimeShift="1ms"`,
		},
	})
	s.T().Log("server started")
	s.Require().NoError(err)
	s.server = srv
	s.client = srv.Client()
}

func (s *Suite) TearDownSuite() {
	if s.worker != nil {
		s.worker.Stop()
	}
	var dumperr error
	if s.client != nil && s.options.HistoriesDir != "" {
		if err := dumpAllHistories(context.Background(), s.client, s.options.HistoriesDir, s.workflowsTypesToReplayTest, s.options.RedactWorkerIdentity); err != nil {
			dumperr = err
		}
	}
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		_ = s.server.Stop()
	}

	s.Require().NoError(dumperr)
}

// RegisterWorkflowForReplay should be called in SetupSuite to
// indicate the workflows for which we want to perform replay testing.
//
// TODO: based on what is registered here, skip history generation for
// tests that do not run any of these workflows.
func (s *Suite) RegisterWorkflowForReplay(w interface{}) {
	if s.workflowsTypesToReplayTest == nil {
		s.workflowsTypesToReplayTest = make(map[string]interface{})
	}
	workflowType, _ := activityFunctionName(w)
	s.workflowsTypesToReplayTest[workflowType] = w
}

// NewDevServerEnvironment returns a wrapping environment. The returned
// *Env delegates every method to a fresh *testsuite.TestWorkflowEnvironment
// (so unit-test semantics are preserved) and additionally mirrors the
// scenario against the suite's shared dev-server worker to produce a real
// history.
func (s *Suite) NewDevServerEnvironment() *Env {
	idx := atomic.AddInt64(&s.envCount, 1)
	return &Env{
		TestWorkflowEnvironment: s.NewTestWorkflowEnvironment(),
		suite:                   s,
		envIdx:                  idx,
		stubs:                   map[string][]stubCall{},
		stubFnRef:               map[string]interface{}{},
		workflowStubs:           map[string][]stubCall{},
		workflowStubFnRef:       map[string]interface{}{},
	}
}

// ensureWorkerCreated lazily creates the shared worker. The worker registers a
// dynamic activity handler that routes every activity call to the right Env's
// stub table via the workflow ID, a dynamic workflow handler that does the
// same for child workflows via the parent workflow ID, and a workflow outbound
// interceptor that clamps retry-policy intervals and timer durations to 1ms
// and rewrites every activity's task queue to sharedTaskQueue.
func (s *Suite) ensureWorkerCreated() error {
	s.workerCreateOnce.Do(func() {
		s.worker = worker.New(s.client, sharedTaskQueue, worker.Options{
			Interceptors: []interceptor.WorkerInterceptor{&fastReplayInterceptor{}},
		})

		s.worker.RegisterDynamicActivity(
			func(ctx context.Context, payloads converter.EncodedValues) (interface{}, error) {
				return s.dynamicActivityHandler(ctx, payloads)
			},
			activity.DynamicRegisterOptions{},
		)

		s.worker.RegisterDynamicWorkflow(
			func(ctx workflow.Context, payloads converter.EncodedValues) (interface{}, error) {
				return s.dynamicWorkflowHandler(ctx, payloads)
			},
			workflow.DynamicRegisterOptions{},
		)
	})
	return s.workerCreateErr
}

// startWorkerOnce lazily starts the shared worker. Workflows should be
// registered before this is called so the worker does not poll a workflow task
// before its type is available in the registry.
func (s *Suite) startWorkerOnce() error {
	if err := s.ensureWorkerCreated(); err != nil {
		return err
	}

	s.workerStartOnce.Do(func() {
		if err := s.worker.Start(); err != nil {
			s.workerStartErr = fmt.Errorf("worker start: %w", err)
			return
		}
	})
	return s.workerStartErr
}

func (s *Suite) replayAndDeleteHistories(
	workflowType string,
	workflowFn interface{},
) error {
	if s.options.HistoriesDir == "" || workflowType == "" {
		return nil
	}

	dir := filepath.Join(s.options.HistoriesDir, sanitize(workflowType))
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

// dynamicActivityHandler is the catch-all activity registered on the
// shared worker. It looks up the current Env by workflow ID, decodes the
// activity payload using the type info captured at OnActivity time, and
// returns the matching stub's recorded result.
func (s *Suite) dynamicActivityHandler(ctx context.Context, payloads converter.EncodedValues) (interface{}, error) {
	info := activity.GetInfo(ctx)
	wfID := info.WorkflowExecution.ID
	actName := info.ActivityType.Name

	raw, ok := s.envsByWFID.Load(wfID)
	if !ok {
		return nil, fmt.Errorf("replaysuite: no env registered for workflow %s (activity %s)", wfID, actName)
	}
	env := raw.(*Env)

	stubFn, ok := env.stubFnRef[actName]
	if !ok {
		return nil, fmt.Errorf("replaysuite: no stub for activity %s in workflow %s", actName, wfID)
	}

	decoded, err := decodePayloadsViaFnType(payloads, stubFn)
	if err != nil {
		return nil, fmt.Errorf("decode payloads for %s: %w", actName, err)
	}

	for _, c := range env.stubs[actName] {
		if stubCallMatches(c, decoded) {
			return extractReturn(c.returns)
		}
	}
	return nil, fmt.Errorf("replaysuite: no stub matched activity %s args=%v", actName, decoded)
}

// dynamicWorkflowHandler is the catch-all workflow registered on the shared
// worker. Child workflows started via ExecuteChildWorkflow land here — the
// parent workflow under replay is explicitly registered, so it keeps being
// served by its real implementation, while child workflows are not registered
// on the shared worker and so fall through to this handler. It looks up the
// parent's Env by parent workflow ID, decodes the child's payload using the
// type info captured at OnWorkflow time, and returns the matching stub's
// recorded result.
func (s *Suite) dynamicWorkflowHandler(ctx workflow.Context, payloads converter.EncodedValues) (interface{}, error) {
	info := workflow.GetInfo(ctx)
	wfType := info.WorkflowType.Name

	parent := info.ParentWorkflowExecution
	if parent == nil {
		return nil, fmt.Errorf("replaysuite: workflow %s has no parent; only mocked child workflows are served by the dynamic handler", wfType)
	}

	raw, ok := s.envsByWFID.Load(parent.ID)
	if !ok {
		return nil, fmt.Errorf("replaysuite: no env registered for parent workflow %s (child %s)", parent.ID, wfType)
	}
	env := raw.(*Env)

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
