package replaysuite

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
)

func WriteArchiveFromHistories(w io.Writer, historiesDir string) error {
	root, err := os.OpenRoot(historiesDir)
	if err != nil {
		return fmt.Errorf("opening %s: %w", historiesDir, err)
	}
	defer root.Close()

	fsys := root.FS()
	tw := tar.NewWriter(w)
	err = fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		// Assume that every file represents a history. It may be a good idea
		// to write under a custom .hist extension to avoid potentially testing
		// files that aren't actually histories/
		if d.IsDir() {
			return nil
		}
		return writeHistoryFile(tw, fsys, name)
	})
	if err != nil {
		return fmt.Errorf("walking histories dir: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("closing tar writer: %w", err)
	}
	return nil
}

func writeHistoryFile(tw *tar.Writer, fsys fs.FS, name string) error {
	f, err := fsys.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	err = tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Mode:     0o644,
		Size:     info.Size(),
	})
	if err != nil {
		return err
	}
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("copying %s: %w", name, err)
	}
	return nil
}
