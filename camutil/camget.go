/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file is copied from camlistore.org/cmd/camget/camget.got
// Version 43e0b72ec49ac9a0eac60392a68b11eef095374f

package camutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/osutil"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/types"
)

// A little less than the sniffer will take, so we don't truncate.
const sniffSize = 900 * 1024

// smartFetch the things that blobs point to, not just blobs.
func smartFetch(src blob.Fetcher, targ string, br blob.Ref) error {
	rc, err := fetch(src, br)
	if err != nil {
		return err
	}
	rcc := types.NewOnceCloser(rc)
	defer rcc.Close()

	sniffer := index.NewBlobSniffer(br)
	_, err = io.CopyN(sniffer, rc, sniffSize)
	if err != nil && err != io.EOF {
		return err
	}

	sniffer.Parse()
	b, ok := sniffer.SchemaBlob()

	if !ok {
		if Verbose {
			Log.Info("Fetching opaque data", "blob", br, "destination", targ)
		}

		// opaque data - put it in a file
		f, err := os.Create(targ)
		if err != nil {
			return fmt.Errorf("opaque: %v", err)
		}
		defer f.Close()
		body, _ := sniffer.Body()
		r := io.MultiReader(bytes.NewReader(body), rc)
		_, err = io.Copy(f, r)
		return err
	}
	rcc.Close()

	switch b.Type() {
	case "directory":
		dir := filepath.Join(targ, b.FileName())
		if Verbose {
			Log.Info("Fetching directory", "blob", br, "destination", dir)
		}
		if err := os.MkdirAll(dir, b.FileMode()); err != nil {
			return err
		}
		if err := setFileMeta(dir, b); err != nil {
			Log.Error("setFileMeta", "error", err)
		}
		entries, ok := b.DirectoryEntries()
		if !ok {
			return fmt.Errorf("bad entries blobref in dir %v", b.BlobRef())
		}
		return smartFetch(src, dir, entries)
	case "static-set":
		if Verbose {
			Log.Info("Fetching directory entries", "blob", br, "destination", targ)
		}

		// directory entries
		const numWorkers = 10
		type work struct {
			br   blob.Ref
			errc chan<- error
		}
		members := b.StaticSetMembers()
		workc := make(chan work, len(members))
		defer close(workc)
		for i := 0; i < numWorkers; i++ {
			go func() {
				for wi := range workc {
					wi.errc <- smartFetch(src, targ, wi.br)
				}
			}()
		}
		var errcs []<-chan error
		for _, mref := range members {
			errc := make(chan error, 1)
			errcs = append(errcs, errc)
			workc <- work{mref, errc}
		}
		for _, errc := range errcs {
			if err := <-errc; err != nil {
				return err
			}
		}
		return nil
	case "file":
		fr, err := schema.NewFileReader(src, br)
		if err != nil {
			return fmt.Errorf("NewFileReader: %v", err)
		}
		fr.LoadAllChunks()
		defer fr.Close()

		name := filepath.Join(targ, b.FileName())

		if fi, err := os.Stat(name); err == nil && fi.Size() == fr.Size() {
			if Verbose {
				Log.Info("Skipping (already exists).", "file", name)
			}
			return nil
		}

		if Verbose {
			Log.Info("Writing", "blob", br, "destination", name)
		}

		f, err := os.Create(name)
		if err != nil {
			return fmt.Errorf("file type: %v", err)
		}
		defer f.Close()
		if _, err := io.Copy(f, fr); err != nil {
			return fmt.Errorf("Copying %s to %s: %v", br, name, err)
		}
		if err := setFileMeta(name, b); err != nil {
			Log.Error("setFileMeta", "error", err)
		}
		return nil
	case "symlink":
		if SkipIrregular {
			return nil
		}
		sf, ok := b.AsStaticFile()
		if !ok {
			return errors.New("blob is not a static file")
		}
		sl, ok := sf.AsStaticSymlink()
		if !ok {
			return errors.New("blob is not a symlink")
		}
		name := filepath.Join(targ, sl.FileName())
		if _, err := os.Lstat(name); err == nil {
			if Verbose {
				Log.Info("Skipping creating symbolic link " + name + ": A file with that name exists")
			}
			return nil
		}
		target := sl.SymlinkTargetString()
		if target == "" {
			return errors.New("symlink without target")
		}

		// On Windows, os.Symlink isn't yet implemented as of Go 1.3.
		// See https://code.google.com/p/go/issues/detail?id=5750
		err := os.Symlink(target, name)
		// We won't call setFileMeta for a symlink because:
		// the permissions of a symlink do not matter and Go's
		// os.Chtimes always dereferences (does not act on the
		// symlink but its target).
		return err
	case "fifo":
		if SkipIrregular {
			return nil
		}
		name := filepath.Join(targ, b.FileName())

		sf, ok := b.AsStaticFile()
		if !ok {
			return errors.New("blob is not a static file")
		}
		_, ok = sf.AsStaticFIFO()
		if !ok {
			return errors.New("blob is not a static FIFO")
		}

		if _, err := os.Lstat(name); err == nil {
			Log.Info("Skipping FIFO " + name + ": A file with that name already exists")
			return nil
		}

		err = osutil.Mkfifo(name, 0600)
		if err == osutil.ErrNotSupported {
			Log.Warn("Skipping FIFO " + name + ": Unsupported filetype")
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: osutil.Mkfifo(): %v", name, err)
		}

		if err := setFileMeta(name, b); err != nil {
			Log.Error("setFileMeta", "error", err)
		}

		return nil

	case "socket":
		if SkipIrregular {
			return nil
		}
		name := filepath.Join(targ, b.FileName())

		sf, ok := b.AsStaticFile()
		if !ok {
			return errors.New("blob is not a static file")
		}
		_, ok = sf.AsStaticSocket()
		if !ok {
			return errors.New("blob is not a static socket")
		}

		if _, err := os.Lstat(name); err == nil {
			Log.Info("Skipping socket " + name + ": A file with that name already exists")
			return nil
		}

		err = osutil.Mksocket(name)
		if err == osutil.ErrNotSupported {
			Log.Warn("Skipping socket " + name + ": Unsupported filetype")
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}

		if err := setFileMeta(name, b); err != nil {
			Log.Error("setFileMeta", "error", err)
		}

		return nil

	default:
		return errors.New("unknown blob type: " + b.Type())
	}
	panic("unreachable")
}

func setFileMeta(name string, blob *schema.Blob) error {
	err1 := os.Chmod(name, blob.FileMode())
	var err2 error
	if mt := blob.ModTime(); !mt.IsZero() {
		err2 = os.Chtimes(name, mt, mt)
	}
	// TODO: we previously did os.Chown here, but it's rarely wanted,
	// then the schema.Blob refactor broke it, so it's gone.
	// Add it back later once we care?
	for _, err := range []error{err1, err2} {
		if err != nil {
			return err
		}
	}
	return nil
}
