// Copyright 2013, 2023 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"

	"perkeep.org/pkg/auth"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/localdisk"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"
)

var logger *slog.Logger

// SetLogger sets the package-level *slog.Logger
func SetLogger(lgr *slog.Logger) { logger = lgr }

// Downloader is the struct for downloading file/dir blobs
type Downloader struct {
	cl *client.Client
	blob.Fetcher
	args []string
}

var (
	cachedClient    = make(map[string]*client.Client, 1)
	cachedClientMtx sync.Mutex
)

type Option func(*clientOptions)

type clientOptions struct {
	UseRetryTransport bool
}

func (c *clientOptions) apply(opts ...Option) {
	for _, o := range opts {
		o(c)
	}
}
func WithRetryTransport(b bool) Option { return func(o *clientOptions) { o.UseRetryTransport = b } }

// NewClient returns a new client for the given server. Auth is set up according
// to the client config (~/.config/camlistore/client-config.json)
// and the environment variables.
func NewClient(server string, clientOpts ...Option) (*client.Client, error) {
	if server == "" {
		server = "http://localhost:3179"
	}
	cachedClientMtx.Lock()
	defer cachedClientMtx.Unlock()
	c, ok := cachedClient[server]
	if ok {
		return c, nil
	}
	var authIncluded bool
	opts := make([]client.ClientOption, 0, 4)
	if !strings.Contains(server, "://") {
		opts = append(opts, client.OptionServer(server), client.OptionInsecure(true))
	} else if strings.HasPrefix(server, "file://") {
		bs, err := localdisk.New(server[7:])
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.OptionUseStorageClient(bs))
	} else {
		URL, err := url.Parse(server)
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.OptionServer(server))
		if URL.Scheme == "http" {
			opts = append(opts, client.OptionInsecure(true))
		}
		if u := URL.User; u != nil && u.Username() != "" {
			passwd, _ := u.Password()
			opts = append(opts, client.OptionAuthMode(auth.NewBasicAuth(u.Username(), passwd)))
			authIncluded = true
		}
	}
	var err error
	if c, err = client.New(opts...); err != nil {
		return nil, err
	}
	var options clientOptions
	options.apply(clientOpts...)

	if options.UseRetryTransport {
		cl := c.HTTPClient()
		cl.Transport = retryTransport{tr: cl.Transport, Strategy: defaultStrategy}
		c.SetHTTPClient(cl)
	}
	if !authIncluded {
		if err = c.SetupAuth(); err != nil {
			return nil, err
		}
	}
	cachedClient[server] = c
	return c, nil
}

var (
	// nosemgrep: trailofbits.go.iterate-over-empty-map.iterate-over-empty-collection
	cachedDownloader    = make(map[string]*Downloader, 1)
	cachedDownloaderMtx sync.Mutex
)

// The followings are copied from camlistore.org/cmd/camget

// NewDownloader creates a new Downloader (client + properties + disk cache)
// for the server
func NewDownloader(server string, noCache bool) (*Downloader, error) {
	cachedDownloaderMtx.Lock()
	defer cachedDownloaderMtx.Unlock()
	down, ok := cachedDownloader[server]
	if ok {
		return down, nil
	}

	cl, err := NewClient(server)
	if err != nil {
		return nil, err
	}
	down = &Downloader{cl: cl}

	if strings.HasPrefix(server, "file://") {
		down.Fetcher = down.cl
		cachedDownloader[server] = down
		return down, nil
	}

	if noCache {
		down.Fetcher = down.cl
	} else {
		bc, err := NewBadgerCache(down.cl, 512<<20)
		if err != nil {
			return nil, fmt.Errorf("setup local disk cache: %w", err)
		}
		logger.Debug("Using temp blob cache directory " + bc.Root)
		down.Fetcher = bc
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
	if down != nil && down.Fetcher != nil {
		if dc, ok := down.Fetcher.(interface{ Clean() }); ok {
			dc.Clean()
		}
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
		err = fmt.Errorf("cannot decode %q as base64: %w", t[i+1:], err)
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
		err = fmt.Errorf("cannot parse %q as blobref: %w", arg, err)
		return
	}
	return br, nil
}

// Start starts the downloads of the blobrefs.
// Just the JSON schema if contents is false, else the content of the blob.
func (down *Downloader) Start(ctx context.Context, contents bool, items ...blob.Ref) (io.ReadCloser, error) {
	readers := make([]io.Reader, 0, len(items))
	closers := make([]io.Closer, 0, len(items))
	var (
		rc  io.ReadCloser
		err error
	)
	for _, br := range items {
		if contents {
			rc, err = schema.NewFileReader(ctx, down.Fetcher, br)
			if err == nil {
				rc.(*schema.FileReader).LoadAllChunks()
			}
		} else {
			var b *blob.Blob
			b, err = blob.FromFetcher(ctx, down.Fetcher, br)
			if err == nil {
				var r io.Reader
				r, err = b.ReadAll(ctx)
				rc = struct {
					io.Reader
					io.Closer
				}{r, io.NopCloser(nil)}
			} else if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%v: %w", br, err)
			} else {
				logger.Error("blob.FromFetcher", "br", br, "error", err)
			}
		}
		if err == nil && rc != nil {
			readers = append(readers, rc)
			closers = append(closers, rc)
			continue
		}
		logger.Info("downloading", "blob", br, "error", err)
		args := append(make([]string, 0, len(down.args)+3), down.args...)
		if contents {
			args = append(args, "-contents=true")
		}
		if InsecureTLS {
			args = append(args, "-insecure=true")
		}
		args = append(args, br.String())
		// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command
		c := exec.CommandContext(ctx, cmdPkGet, args...)
		var errBuf bytes.Buffer
		c.Stderr = &errBuf
		if rc, err = c.StdoutPipe(); err != nil {
			return nil, fmt.Errorf("create stdout pipe for %s %q: %s: %w", cmdPkGet, args, errBuf.String(), err)
		}
		logger.Info("calling "+cmdPkGet, "args", args)
		if err = c.Run(); err != nil {
			return nil, fmt.Errorf("call %s %q: %s: %w", cmdPkGet, args, errBuf.String(), err)
		}
		readers = append(readers, rc)
		closers = append(closers, rc)
	}

	if len(readers) == 0 {
		return nil, io.EOF
	}
	return struct {
		io.Reader
		io.Closer
	}{io.MultiReader(readers...),
		multiCloser{closers},
	}, nil
}

// Save saves contents of the blobs into destDir as files
func (down *Downloader) Save(ctx context.Context, destDir string, contents bool, items ...blob.Ref) error {
	for _, br := range items {
		if err := smartFetch(ctx, down.Fetcher, destDir, br); err != nil {
			logger.Error("Save", "error", err)
			return err
		}
	}
	return nil
}

func fetch(ctx context.Context, src blob.Fetcher, br blob.Ref) (io.ReadCloser, error) {
	r, _, err := src.Fetch(ctx, br)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", br, err)
	}
	return r, nil
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
