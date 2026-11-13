package replaysuite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

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
	fmt.Println(key)
}

func (s *ReplaySuite) Replay() error {

	// TODO: replace with target download.
	root := filepath.Join(os.Getenv(envHistoriesDir), defaultHistoriesDir)

	var errs []error
	for _, key := range slices.Sorted(maps.Keys(s.workflows)) {
		dir := filepath.Join(root, key)
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("read histories for workflow %s: %w", key, err))
			continue
		}

		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(s.workflows[key])

		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			historyFile := filepath.Join(dir, entry.Name())
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, historyFile); err != nil {
				errs = append(errs, fmt.Errorf("replay %s: %w", historyFile, err))
			}
		}
	}
	return errors.Join(errs...)
}
