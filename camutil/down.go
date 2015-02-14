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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sync"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/cacher"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/schema"

	"gopkg.in/inconshreveable/log15.v2"
)

// Log is discarded by default. Use Log.SetHandler to set destionation.
var Log = log15.New("lib", "camutil")

func init() {
	Log.SetHandler(log15.DiscardHandler())
}

// Downloader is the struct for downloading file/dir blobs
type Downloader struct {
	cl   *client.Client
	dc   *cacher.DiskCache
	args []string
}

var (
	cachedClient    = make(map[string]*client.Client, 1)
	cachedClientMtx sync.Mutex
)

// NewClient returns a new client for the given server. Auth is set up according
// to the client config (~/.config/camlistore/client-config.json)
// and the environment variables.
func NewClient(server string) (*client.Client, error) {
	if server == "" {
		server = "localhost:3179"
	}
	cachedClientMtx.Lock()
	defer cachedClientMtx.Unlock()
	c, ok := cachedClient[server]
	if ok {
		return c, nil
	}
	c = client.New(server)
	if err := c.SetupAuth(); err != nil {
		return nil, err
	}
	cachedClient[server] = c
	return c, nil
}

var (
	cachedDownloader    = make(map[string]*Downloader, 1)
	cachedDownloaderMtx sync.Mutex
)

// The followings are copied from camlistore.org/cmd/camget

// NewDownloader creates a new Downloader (client + properties + disk cache)
// for the server
func NewDownloader(server string) (*Downloader, error) {
	cachedDownloaderMtx.Lock()
	defer cachedDownloaderMtx.Unlock()
	down, ok := cachedDownloader[server]
	if ok {
		return down, nil
	}

	down = new(Downloader)
	var err error
	if down.cl, err = NewClient(server); err != nil {
		return nil, err
	}

	down.cl.InsecureTLS = InsecureTLS
	tr := down.cl.TransportForConfig(&client.TransportConfig{
		Verbose: Verbose,
	})
	down.cl.SetHTTPClient(&http.Client{Transport: tr})

	down.dc, err = cacher.NewDiskCache(down.cl)
	if err != nil {
		return nil, fmt.Errorf("Error setting up local disk cache: %v", err)
	}
	if Verbose {
		Log.Info("Using temp blob cache directory " + down.dc.Root)
	}
	if server != "" {
		down.args = []string{"-server=" + server}
	} else {
		down.args = []string{}
	}

	cachedDownloader[server] = down
	return down, nil
}

// Close closes the downloader (the underlying client)
func (down *Downloader) Close() {
	if down != nil && down.dc != nil {
		down.dc.Clean()
	}
}

// ParseBlobNames parses the blob names, appending to items, and returning
// the expanded slice, and error if happened.
// This uses blob.Parse, and can decode base64-encoded refs as a plus.
func ParseBlobNames(items []blob.Ref, names []string) ([]blob.Ref, error) {
	for _, arg := range names {
		br, ok := blob.Parse(arg)
		if !ok {
			var e error
			if br, e = Base64ToRef(arg); e != nil {
				return nil, e
			}
		}
		items = append(items, br)
	}
	return items, nil
}

// Base64ToRef decodes a base64-encoded blobref
func Base64ToRef(arg string) (br blob.Ref, err error) {
	b := make([]byte, 64)
	t := make([]byte, 2*len(b))
	var i, n int
	i = len(arg)
	if i > cap(t) {
		i = cap(t)
	}
	t = []byte(arg[:i])
	i = bytes.IndexByte(t, byte('-'))
	if i < 0 {
		err = fmt.Errorf("no - in %q", arg)
		return
	}
	n, err = base64.URLEncoding.Decode(b[:cap(b)], t[i+1:])
	if err != nil {
		err = fmt.Errorf("cannot decode %q as base64: %s", t[i+1:], err)
		return
	}
	b = b[:n]
	copy(t[:i], bytes.ToLower(t[:i]))
	t = t[:cap(t)]
	n = 2*len(b) - len(t) + n + 1
	if n > 0 {
		t = append(t, make([]byte, n)...)
	}
	n = hex.Encode(t[i+1:], b)
	arg = string(t[:i+1+n])
	br, ok := blob.Parse(arg)
	if !ok {
		err = fmt.Errorf("cannot parse %q as blobref", arg)
		return
	}
	return br, nil
}

// Start starts the downloads of the blobrefs.
// Just the JSON schema if contents is false, else the content of the blob.
func (down *Downloader) Start(contents bool, items ...blob.Ref) (io.ReadCloser, error) {
	readers := make([]io.Reader, 0, len(items))
	closers := make([]io.Closer, 0, len(items))
	var (
		rc  io.ReadCloser
		err error
	)
	for _, br := range items {
		if contents {
			rc, err = schema.NewFileReader(down.dc, br)
			if err == nil {
				rc.(*schema.FileReader).LoadAllChunks()
			}
		} else {
			rc, err = fetch(down.dc, br)
		}
		if err == nil {
			readers = append(readers, rc)
			closers = append(closers, rc)
			continue
		}
		Log.Error("downloading", "blog", br, "error", err)
		args := append(make([]string, 0, len(down.args)+3), down.args...)
		if contents {
			args = append(args, "-contents=true")
		}
		if InsecureTLS {
			args = append(args, "-insecure=true")
		}
		args = append(args, br.String())
		c := exec.Command("camget", args...)
		errbuf := bytes.NewBuffer(nil)
		c.Stderr = errbuf
		if rc, err = c.StdoutPipe(); err != nil {
			return nil, fmt.Errorf("error createing stdout pipe for camget %q: %s (%v)", args, errbuf.String(), err)
		}
		Log.Info("calling camget", "args", args)
		if err = c.Run(); err != nil {
			return nil, fmt.Errorf("error calling camget %q: %s (%v)", args, errbuf.String(), err)
		}
		readers = append(readers, rc)
		closers = append(closers, rc)
	}

	return struct {
		io.Reader
		io.Closer
	}{io.MultiReader(readers...),
		multiCloser{closers},
	}, nil
}

// Save saves contents of the blobs into destDir as files
func (down *Downloader) Save(destDir string, contents bool, items ...blob.Ref) error {
	for _, br := range items {
		if err := smartFetch(down.dc, destDir, br); err != nil {
			Log.Crit("Save", "error", err)
			return err
		}
	}
	return nil
}

func fetch(src blob.Fetcher, br blob.Ref) (r io.ReadCloser, err error) {
	if Verbose {
		Log.Debug("Fetch", "blob", br)
	}
	r, _, err = src.Fetch(br)
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch %s: %s", br, err)
	}
	return r, err
}

var _ = io.Closer(multiCloser{})

type multiCloser struct {
	closers []io.Closer
}

func (mc multiCloser) Close() error {
	var err error
	for _, c := range mc.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}
