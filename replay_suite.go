package replaysuite

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"strings"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type ReplaySuite struct {
	workflows map[string]any
}

// Register will register a workflow function for replay testing. The suite will
// only run replay tests for the registered workflows and will skip history
// files that do not correspond to a registered workflow (using the mapping
// between workflow function and its associated string name).
//
// If you think that a workflow package is likely to change, consider
// registering a workflow with an alias, such that the alias can be used as a
// string identifier for the workflow instead of its package name. Note that
// this may require changes to an existing test suite that does not use an
//
// Note that in this context the word "registration" has a different meaning
// from workflow or activity registration on a worker or a test envirionment.
//
// To configure the behavior of Register, see RegisterWithOptions.
func (s *ReplaySuite) Register(w any) {
	s.RegisterWithOptions(w, RegisterOptions{})
}

// RegisterOptions configures how a workflow is registered for replay testing.
type RegisterOptions struct {
	// If set, the replay suite will use this folder to read histories for the
	// workflow function being registered. In order for this to work properly,
	// all your test workflow environments that run that specific function must
	// "SetWorkflowFolder" with this location.
	WorkflowFolder string
}

func (s *ReplaySuite) RegisterWithOptions(w any, options RegisterOptions) {
	if s.workflows == nil {
		s.workflows = make(map[string]any)
	}
	key := workflowKey(w)
	if options.WorkflowFolder != "" {
		key = workflowKey(options.WorkflowFolder)
	}
	if _, ok := s.workflows[key]; ok {
		panic(fmt.Sprintf("a function resolving to identifier %q is already registered", key))
	}
	s.workflows[key] = w
	fmt.Println(key)
}

// Replay runs the replay tests against a target archive.
func (s *ReplaySuite) Replay(r io.Reader) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading next element from archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		// find the workflow key, which is the top-level folder name
		key, _, ok := strings.Cut(hdr.Name, "/")
		if !ok {
			continue
		}
		// skip replay for workflows that have histories, but aren't registered.
		wf, ok := s.workflows[key]
		if !ok {
			log.Printf("warning: skipping %s, no workflow registered for %q", hdr.Name, key)
			continue
		}

		hist, err := client.HistoryFromJSON(tr, client.HistoryJSONOptions{})
		if err != nil {
			return fmt.Errorf("decoding %s: %w", hdr.Name, err)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(wf)
		if err := replayer.ReplayWorkflowHistory(nil, hist); err != nil {
			return fmt.Errorf("replaying %s: %w", hdr.Name, err)
		}
	}
	return nil
}
