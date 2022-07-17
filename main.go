// Copyright 2013, 2022 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/memory"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/peterbourgon/ff/v3/ffcli"
	"github.com/tgulacsi/camproxy/camutil"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var zl = zerolog.New(os.Stderr)
var logger = zerologr.New(&zl)

var (
	fs                = flag.NewFlagSet("serve", flag.ContinueOnError)
	flagVerbose       = fs.Bool("v", false, "verbose logging")
	flagInsecureTLS   = fs.Bool("k", camutil.InsecureTLS, "allow insecure TLS")
	flagSkipIrregular = fs.Bool("skip-irregular", camutil.SkipIrregular, "skip irregular files")
	//flagServer      = fs.String("server", ":3179", "Camlistore server address")
	flagNoCache       = fs.Bool("no-cache", true, "no disk cache")
	flagCapCtime      = fs.Bool("capctime", false, "forge ctime to be less or equal to mtime")
	flagNoAuth        = fs.Bool("noauth", false, "no HTTP Basic Authentication, even if CAMLI_AUTH is set")
	flagListen        = fs.String("listen", ":3178", "listen on")
	flagParanoid      = fs.String("paranoid", "", "Paranoid mode: save uploaded files also under this dir")
	flagSkipHaveCache = fs.Bool("skiphavecache", false, "Skip have cache? (more stress on camlistored)")

	server string
)

func main() {
	if err := Main(); err != nil {
		logger.Error(err, "ERROR")
		os.Exit(1)
	}
}

func Main() error {
	client.AddFlags() // add -server flag

	serveCmd := ffcli.Command{Name: "serve", FlagSet: fs,
		Exec: func(ctx context.Context, args []string) error {
			server = client.ExplicitServer()
			camutil.InsecureTLS = *flagInsecureTLS
			camutil.SkipIrregular = *flagSkipIrregular
			s := &http.Server{
				Addr:           *flagListen,
				Handler:        http.HandlerFunc(handle),
				ReadTimeout:    300 * time.Second,
				WriteTimeout:   300 * time.Second,
				MaxHeaderBytes: 1 << 20,
			}
			if !*flagNoAuth {
				camliAuth := os.Getenv("CAMLI_AUTH")
				if camliAuth != "" {
					s.Handler = camutil.SetupBasicAuthChecker(handle, camliAuth)
				}
			}
			defer func() {
				camutil.Close()
			}()
			mimeCache = camutil.NewMimeCache(filepath.Join(os.TempDir(),
				"mimecache-"+os.Getenv("BRUNO_CUS")+"_"+os.Getenv("BRUNO_ENV")+".kv"),
				0)
			defer mimeCache.Close()
			logger.Info("Listening", "http", s.Addr, "camlistore", server)
			return s.ListenAndServe()
		},
	}

	refCmd := ffcli.Command{Name: "ref",
		Exec: func(ctx context.Context, args []string) error {
			var ref string
			if len(args) == 0 || args[0] == "" || args[0] == "-" {
				b, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				ref = string(b)
			} else {
				ref = args[0]
			}
			br, err := camutil.Base64ToRef(ref)
			if err == nil {
				_, err = fmt.Println(br)
			} else if br, ok := blob.Parse(ref); ok {
				_, err = fmt.Println(camutil.RefToBase64(br))
			}
			return err
		},
	}

	upBytesCmd := ffcli.Command{Name: "upbytes",
		Exec: func(ctx context.Context, args []string) error {
			server = client.ExplicitServer()
			r := io.ReadCloser(os.Stdin)
			if !(len(args) == 0 || args[0] == "" || args[0] == "-") {
				var err error
				if r, err = os.Open(args[0]); err != nil {
					return err
				}
			}
			defer r.Close()
			up, err := getUploader()
			if err != nil {
				return err
			}
			defer up.Close()
			br, err := up.UploadBytes(ctx, r)
			if err != nil {
				return err
			}
			_, err = fmt.Println(br)
			return err
		},
	}

	hshCmd := ffcli.Command{Name: "hash", FlagSet: flag.NewFlagSet("hash", flag.ContinueOnError),
		Exec: func(ctx context.Context, args []string) error {
			var mem memory.Storage
			for _, fn := range args {
				fh := os.Stdin
				if fn == "" || fn == "-" {
					fn = "<stdin>"
				} else {
					var err error
					if fh, err = os.Open(fn); err != nil {
						return err
					}
					fn = filepath.Base(fn)
				}
				br, err := schema.WriteFileFromReader(ctx, &mem, fn, fh)
				fh.Close()
				if err != nil {
					return err
				}
				fmt.Println("---", br, "---")
				s, _ := mem.BlobContents(br)
				fmt.Println(s)
			}
			return nil
		},
	}
	flagUseSHA1 := hshCmd.FlagSet.Bool("use-sha1", false, "Force use of sha1")

	app := ffcli.Command{Name: "camutil", FlagSet: flag.CommandLine,
		Exec: func(ctx context.Context, args []string) error {
			return serveCmd.Exec(ctx, args)
		},
		Subcommands: []*ffcli.Command{&serveCmd, &refCmd, &hshCmd, &upBytesCmd},
	}

	if err := app.Parse(os.Args[1:]); err != nil {
		return err
	}

	if *flagVerbose {
		camutil.SetLogger(logger.WithValues("lib", "camutil"))
	}

	if *flagUseSHA1 {
		os.Setenv("CAMLI_SHA1_ENABLED", "1")
		os.Setenv("PK_TEST_USE_SHA1", "1")
	}

	ctx, cancel := wrapCtx(context.Background())
	defer cancel()
	return app.Run(ctx)
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "empty request", 400)
		return
	}
	if r.Body != nil {
		defer r.Body.Close()
	}
	values := r.URL.Query()

	switch r.Method {
	case "GET":
		// the path is treated as a blobname
		items, err := camutil.ParseBlobNames(nil, []string{r.URL.Path[1:]})
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if len(items) == 0 {
			http.Error(w, "a blobref is needed!", 400)
			return
		}
		content := values.Get("raw") != "1"
		okMime, nm := "application/json", ""
		if content {
			okMime = values.Get("mimeType")
			if okMime == "" && len(items) == 1 {
				nm = camutil.RefToBase64(items[0])
				okMime = mimeCache.Get(nm)
			}
		}
		d, err := getDownloader()
		if err != nil {
			http.Error(w,
				fmt.Sprintf("error getting downloader to %q: %s", server, err),
				500)
			return
		}
		rc, err := d.Start(r.Context(), content, items...)
		if err != nil {
			http.Error(w, fmt.Sprintf("download error: %v", err), 500)
			return
		}
		defer rc.Close()

		if okMime == "" {
			// must sniff
			var rr io.Reader
			okMime, rr = camutil.MIMETypeFromReader(rc)
			rc = struct {
				io.Reader
				io.Closer
			}{rr, rc}
		}

		rw := newRespWriter(w, nm, okMime)
		defer rw.Close()
		if _, err = io.Copy(rw, rc); err != nil {
			http.Error(w, fmt.Sprintf("error downloading %q: %s", items, err), 500)
			return
		}
		return

	case "POST":
		u, err := getUploader()
		if err != nil {
			http.Error(w, fmt.Sprintf("error getting uploader to %q: %s", server, err), 500)
			return
		}
		dn, err := os.MkdirTemp("", "camproxy")
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot create temporary directory: %s", err), 500)
			return
		}
		var paraSource, paraDest string
		defer func() {
			if paraSource != "" && paraDest != "" { // save at last
				os.MkdirAll(filepath.Dir(paraDest), 0700)
				logger.Info("Paranoid copying", "src", paraSource, "dst", paraDest)
				if err = camutil.LinkOrCopy(paraSource, paraDest); err != nil {
					logger.Info("copying", "src", paraSource, "dst", paraDest, "error", err)
				}
			}
			os.RemoveAll(dn)
		}()

		var filenames, mimetypes []string

		ct := r.Header.Get("Content-Type")
		if ct, _, err = mime.ParseMediaType(ct); err != nil {
			logger.Info("parsing Content-Type", "ct", ct, "error", err)
			if ct == "" {
				ct = r.Header.Get("Content-Type")
			}
		}
		logger.Info("request Content-Type: " + ct)

		switch ct {
		case "multipart/form", "multipart/form-data", "application/x-www-form-urlencoded":
			mr, mrErr := r.MultipartReader()
			if mrErr != nil {
				http.Error(w, fmt.Sprintf("error parsing request body as multipart/form: %s", mrErr), 400)
				return
			}
			qmtime := values.Get("mtime")
			if qmtime == "" {
				qmtime = r.Header.Get("Last-Modified")
			}
			filenames, mimetypes, err = saveMultipartTo(dn, mr, qmtime)
		default: // legacy direct upload
			var fn, mime string
			fn, mime, err = saveDirectTo(dn, r)
			if fn != "" {
				filenames = append(filenames, fn)
				mimetypes = append(mimetypes, mime)
			}
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		logger.Info("uploading", "files", filenames, "mime-types", mimetypes)

		short := values.Get("short") == "1"
		var attrs map[string]string
		if values.Get("noperma") != "1" { // create permanode, iff attrs present
			attrs = make(map[string]string, len(values))
			for k, vv := range values {
				if !strings.HasPrefix(k, "a.") {
					continue
				}
				k = k[2:]
				if strings.HasPrefix(k, "camli") {
					continue
				}
				for _, v := range vv {
					attrs[k] = v
					break
				}
			}
		}

		var content, perma blob.Ref
		switch len(filenames) {
		case 0:
			http.Error(w, "no files in request", 400)
			return
		case 1:
			content, perma, err = u.UploadFileLazyAttr(r.Context(), filenames[0], mimetypes[0], attrs)
		default:
			content, perma, err = u.UploadFileLazyAttr(r.Context(), dn, "", attrs)
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("error uploading %q: %s", filenames, err), 500)
			return
		}
		// store mime types
		shortKey := camutil.RefToBase64(content)
		if len(filenames) == 1 {
			if len(mimetypes) == 1 && mimetypes[0] != "" {
				mimeCache.Set(shortKey, mimetypes[0])
			}
			if *flagParanoid != "" {
				paraSource, paraDest = filenames[0], getParanoidPath(content)
			}
		}
		w.Header().Add("Content-Type", "text/plain")
		b := bytes.NewBuffer(make([]byte, 0, 128))
		if short {
			b.WriteString(shortKey)
		} else {
			b.WriteString(content.String())
		}
		if perma.Valid() {
			b.Write([]byte{'\n'})
			if short {
				b.WriteString(camutil.RefToBase64(perma))
			} else {
				b.WriteString(perma.String())
			}
		}
		w.Header().Add("Content-Length", strconv.Itoa(len(b.Bytes())))
		w.WriteHeader(201)
		w.Write(b.Bytes())
	default:
		http.Error(w, "Method must be GET/POST", http.StatusMethodNotAllowed)
	}
}

func saveDirectTo(destDir string, r *http.Request) (filename, mimeType string, err error) {
	mimeType = r.Header.Get("Content-Type")
	lastmod := parseLastModified(r.Header.Get("Last-Modified"), r.URL.Query().Get("mtime"))
	cd := r.Header.Get("Content-Disposition")
	var fh *os.File
	fn := ""
	if cd != "" {
		_, params, err := mime.ParseMediaType(cd)
		if err != nil {
			logger.Info("parsing Content-Disposition", "cd", cd, "error", err)
		} else {
			fn = params["filename"]
		}
	}
	if fn == "" {
		logger.Info("Cannot determine filename", "content-disposition", cd)
		fh, err = os.CreateTemp(destDir, "file-")
	} else {
		fn = filepath.Join(destDir, safeBaseFn(fn))
		fh, err = os.Create(fn)
	}
	if err != nil {
		return "", "", fmt.Errorf("create temp file %q: %w", fn, err)
	}
	defer fh.Close()
	rdr := io.Reader(r.Body)
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType, rdr = camutil.MIMETypeFromReader(r.Body)
	}
	_, err = io.Copy(fh, rdr)
	if err != nil {
		logger.Info("saving request body", "dst", fh.Name(), "error", err)
	}
	filename = fh.Name()
	if !lastmod.IsZero() {
		if err = os.Chtimes(filename, lastmod, lastmod); err != nil {
			logger.Info("chtimes", "dst", filename, "error", err)
		}
	}
	return
}

func saveMultipartTo(destDir string, mr *multipart.Reader, qmtime string) (filenames, mimetypes []string, err error) {
	var fn string
	var lastmod time.Time
	var part *multipart.Part
	for part, err = mr.NextPart(); err == nil; part, err = mr.NextPart() {
		filename := part.FileName()
		if filename == "" {
			if part.FormName() == "mtime" {
				b := bytes.NewBuffer(make([]byte, 0, 23))
				if _, err = io.CopyN(b, part, 23); err == nil || err == io.EOF {
					qmtime = b.String()
				}
			}
			part.Close()
			continue
		}
		fn = filepath.Join(destDir, safeBaseFn(filename))
		fh, err := os.Create(fn)
		if err != nil {
			part.Close()
			return nil, nil, fmt.Errorf("create temp file %q: %w", fn, err)
		}
		mimeType := part.Header.Get("Content-Type")
		rdr := io.Reader(part)
		if mimeType == "" || mimeType == "application/octet-stream" {
			mimeType, rdr = camutil.MIMETypeFromReader(rdr)
		}
		_, err = io.Copy(fh, rdr)
		if err == nil {
			filenames = append(filenames, fh.Name())
			mimetypes = append(mimetypes, mimeType)
		}
		part.Close()
		closeErr := fh.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
		if err != nil {
			return nil, nil, fmt.Errorf("write %q: %w", fn, err)
		}
		lastmod = parseLastModified(part.Header.Get("Last-Modified"), qmtime)
		if !lastmod.IsZero() {
			if e := os.Chtimes(fn, lastmod, lastmod); e != nil {
				logger.Info("chtimes", "dst", fn, "error", e)
			}
		}
	}
	return filenames, mimetypes, nil
}

func safeBaseFn(filename string) string {
	if i := strings.LastIndexAny(filename, "/\\"); i >= 0 {
		filename = filename[i+1:]
	}
	n := len(filename)
	for strings.IndexByte(filename, '%') >= 0 {
		if fn, err := url.QueryUnescape(filename); err != nil {
			logger.Info("QueryUnescape", "name", filename, "error", err)
			break
		} else {
			filename = fn
		}
		if len(filename) >= n {
			break
		}
		n = len(filename)
	}
	if i := strings.LastIndexAny(filename, "/\\"); i >= 0 {
		filename = filename[i+1:]
	}
	if len(filename) > 255 {
		old := filename
		i := strings.LastIndex(filename, ".")
		ext := ""
		if i >= 0 {
			ext = filename[i:]
		}
		hsh := sha1.New()
		_, _ = io.WriteString(hsh, filename)
		hshS := base64.URLEncoding.EncodeToString(hsh.Sum(nil))
		filename = filename[:255-1-len(hshS)-len(ext)] + "-" + ext
		logger.Info("filename too long", "old", old, "new", filename)
	}
	return filename
}

func parseLastModified(lastModHeader, mtimeHeader string) time.Time {
	var (
		lastmod time.Time
		ok      bool
	)
	if lastModHeader != "" {
		if lastmod, ok = timeParse(lastModHeader); ok {
			return lastmod
		}
	}
	if mtimeHeader == "" {
		return lastmod
	}
	if len(mtimeHeader) >= 23 {
		if lastmod, ok = timeParse(mtimeHeader); ok {
			return lastmod
		}
		logger.Info("too big an mtime " + mtimeHeader + ", and not RFC1123-compliant")
		return lastmod
	}
	if qmt, err := strconv.ParseInt(mtimeHeader, 10, 64); err != nil {
		logger.Info("cannot parse mtime", "header", mtimeHeader, "error", err)
	} else {
		return time.Unix(qmt, 0)
	}
	return lastmod
}

var mimeCache *camutil.MimeCache

type respWriter struct {
	http.ResponseWriter
	name, okMime  string
	buf           []byte
	headerWritten bool
}

func newRespWriter(w http.ResponseWriter, name, okMime string) *respWriter {
	if name != "" && (okMime == "" || okMime == "application/octet-stream") {
		m := mimeCache.Get(name)
		if m != "" {
			okMime = m
		}
	}
	return &respWriter{ResponseWriter: w, name: name, okMime: okMime}
}

func (w *respWriter) Write(p []byte) (int, error) {
	var i int
	if !w.headerWritten {
		if w.okMime == "" || w.okMime == "application/octet-stream" {
			i = len(w.buf)
			w.buf = append(w.buf, p...)
			if len(w.buf) < 1024 {
				return len(p), nil
			}
			w.okMime = camutil.MatchMime(w.okMime, w.buf)
			if w.name != "" && w.okMime != "" {
				mimeCache.Set(w.name, w.okMime)
			}
			p, w.buf = w.buf, nil
		}
		if w.okMime != "" {
			w.ResponseWriter.Header().Add("Content-Type", w.okMime)
		}
		w.ResponseWriter.WriteHeader(200)
		w.headerWritten = true
	}
	n, err := w.ResponseWriter.Write(p)
	return n - i, err
}

func (w *respWriter) Close() (err error) {
	if w.buf != nil && len(w.buf) > 0 {
		_, err = w.ResponseWriter.Write(w.buf)
	}
	return
}

func getUploader() (*camutil.Uploader, error) {
	return camutil.NewUploader(server, *flagCapCtime, *flagSkipHaveCache), nil
}

func getDownloader() (*camutil.Downloader, error) {
	return camutil.NewDownloader(server, *flagNoCache)
}

func getParanoidPath(br blob.Ref) string {
	if *flagParanoid == "" || !br.Valid() {
		return ""
	}
	txt := br.String()
	for i := 0; i < len(txt); i++ {
		if txt[i] == '-' {
			hsh := txt[i+1:]
			return filepath.Join(*flagParanoid, hsh[:3], hsh[3:6], txt+".dat")
		}
	}
	return ""
}

func timeParse(text string) (time.Time, bool) {
	var (
		t   time.Time
		ok  bool
		err error
	)
	for _, pattern := range []string{time.RFC1123, time.UnixDate, time.RFC3339} {
		if t, err = time.Parse(pattern, text); err == nil {
			return t, ok
		}
	}
	return t, false
}

func wrapCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		sig := <-sigCh
		signal.Stop(sigCh)
		cancel()
		if p, _ := os.FindProcess(os.Getpid()); p != nil {
			time.Sleep(time.Second)
			_ = p.Signal(sig)
		}
	}()
	return ctx, cancel
}
