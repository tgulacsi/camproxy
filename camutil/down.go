/*
Copyright 2013 Tamás Gulácsi

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

package camutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/cacher"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/schema"
)

type Downloader struct {
	cl *client.Client
	dc *cacher.DiskCache
}

// copied from camlistore.org/cmd/camget
func NewDownloader() (*Downloader, error) {
	down := &Downloader{cl: client.NewOrFail()}

	down.cl.InsecureTLS = InsecureTLS
	tr := down.cl.TransportForConfig(&client.TransportConfig{
		Verbose: Verbose,
	})
	down.cl.SetHTTPClient(&http.Client{Transport: tr})

	var err error
	down.dc, err = cacher.NewDiskCache(down.cl)
	if err != nil {
		return nil, fmt.Errorf("Error setting up local disk cache: %v", err)
	}
	if Verbose {
		log.Printf("Using temp blob cache directory %s", down.dc.Root)
	}

	return down, nil
}

func (down *Downloader) Close() {
	if down != nil && down.dc != nil {
		down.dc.Clean()
	}
}

func parseBlobNames(items []blob.Ref, names []string) ([]blob.Ref, error) {
	for _, arg := range names {
		br, ok := blob.Parse(arg)
		if !ok {
			return nil, fmt.Errorf("Failed to parse argument %q as a blobref.", arg)
		}
		items = append(items, br)
	}
	return items, nil
}

func (down *Downloader) Download(dest string, contents bool, items ...blob.Ref) error {
	var rc io.ReadCloser
	var err error
	if dest == "" || dest == "-" {
		for _, br := range items {
			if contents {
				rc, err = schema.NewFileReader(down.dc, br)
				if err == nil {
					rc.(*schema.FileReader).LoadAllChunks()
				}
			} else {
				rc, err = fetch(down.dc, br)
			}
			if err != nil {
				log.Fatal(err)
			}
			defer rc.Close()
			if _, err := io.Copy(os.Stdout, rc); err != nil {
				return fmt.Errorf("Failed reading %q: %v", br, err)
			}
		}
	} else {
		for _, br := range items {
			if err := smartFetch(down.dc, dest, br); err != nil {
				log.Fatal(err)
			}
		}
	}
	return nil
}

func fetch(src blob.StreamingFetcher, br blob.Ref) (r io.ReadCloser, err error) {
	if Verbose {
		log.Printf("Fetching %s", br.String())
	}
	r, _, err = src.FetchStreaming(br)
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch %s: %s", br, err)
	}
	return r, err
}

// A little less than the sniffer will take, so we don't truncate.
const sniffSize = 900 * 1024

// smartFetch the things that blobs point to, not just blobs.
func smartFetch(src blob.StreamingFetcher, targ string, br blob.Ref) error {
	rc, err := fetch(src, br)
	if err != nil {
		return err
	}
	defer rc.Close()

	sniffer := index.NewBlobSniffer(br)
	_, err = io.CopyN(sniffer, rc, sniffSize)
	if err != nil && err != io.EOF {
		return err
	}

	sniffer.Parse()
	b, ok := sniffer.SchemaBlob()

	if !ok {
		if Verbose {
			log.Printf("Fetching opaque data %v into %q", br, targ)
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

	switch b.Type() {
	case "directory":
		dir := filepath.Join(targ, b.FileName())
		if Verbose {
			log.Printf("Fetching directory %v into %s", br, dir)
		}
		if err := os.MkdirAll(dir, b.FileMode()); err != nil {
			return err
		}
		if err := setFileMeta(dir, b); err != nil {
			log.Print(err)
		}
		entries, ok := b.DirectoryEntries()
		if !ok {
			return fmt.Errorf("bad entries blobref in dir %v", b.BlobRef())
		}
		return smartFetch(src, dir, entries)
	case "static-set":
		if Verbose {
			log.Printf("Fetching directory entries %v into %s", br, targ)
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
		seekFetcher := blob.SeekerFromStreamingFetcher(src)
		fr, err := schema.NewFileReader(seekFetcher, br)
		if err != nil {
			return fmt.Errorf("NewFileReader: %v", err)
		}
		fr.LoadAllChunks()
		defer fr.Close()

		name := filepath.Join(targ, b.FileName())

		if fi, err := os.Stat(name); err == nil && fi.Size() == fi.Size() {
			if Verbose {
				log.Printf("Skipping %s; already exists.", name)
				return nil
			}
		}

		if Verbose {
			log.Printf("Writing %s to %s ...", br, name)
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
			log.Print(err)
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
