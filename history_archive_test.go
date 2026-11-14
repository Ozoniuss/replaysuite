package replaysuite

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestWriteArchiveFromHistories(t *testing.T) {
	t.Parallel()

	dir := filepath.Join("testdata", "TestWriteArchiveFromHistories")

	var buf bytes.Buffer
	if err := WriteArchiveFromHistories(&buf, dir); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	var names []string
	tr := tar.NewReader(&buf)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read archive: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			t.Fatalf("entry %s has type %q, want regular file", hdr.Name, hdr.Typeflag)
		}
		got, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read entry %s: %v", hdr.Name, err)
		}
		want, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(hdr.Name)))
		if err != nil {
			t.Fatalf("read history %s: %v", hdr.Name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("entry %s content differs from the history file", hdr.Name)
		}
		names = append(names, hdr.Name)
	}

	wantNames := []string{
		"github.com_Ozoniuss_replaysuite.WorkflowSimpleFlow/394d26d2-5e19-476e-847a-528b65d772ea.json",
		"github.com_Ozoniuss_replaysuite.WorkflowWithArgs/3d8dff96-bb81-4293-bb3d-4f5881c601ae.json",
		"github.com_Ozoniuss_replaysuite.WorkflowWithArgs/8feb739f-0215-42b2-87b0-fc35f6d10db2.json",
	}
	if !slices.Equal(names, wantNames) {
		t.Fatalf("archive entries = %v, want %v", names, wantNames)
	}
}
