package replaysuite

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"go.temporal.io/sdk/worker"
)

type HistoryArchiveStore interface {
	Download(ctx context.Context) (io.ReadCloser, error)
	Upload(ctx context.Context, archive io.Reader) error
}

type ReplaySuite struct {
	workflows map[string]any
}

func (s *ReplaySuite) Register(w any) {
	if s.workflows == nil {
		s.workflows = make(map[string]any)
	}
	key := workflowKey(w)
	if _, ok := s.workflows[key]; ok {
		panic(fmt.Sprintf("a workflow resolving to identifier %q is already registered", key))
	}
	s.workflows[key] = w
}

func workflowKey(w any) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(w).Pointer()).Name()
	return strings.TrimSuffix(fullName, "-fm")
}

func replayAndDeleteHistories(
	workflowIdentifier string,
	workflowFn any,
	historiesPath string,
) error {

	dir := filepath.Join(defaultHistoriesDir, sanitize(workflowIdentifier))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read histories for workflow %s: %w", workflowIdentifier, err)
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
