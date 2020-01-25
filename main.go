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

package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/tgulacsi/camproxy/camutil"
)

var logger = log.NewLogfmtLogger(os.Stderr)

var (
	flagVerbose       = flag.Bool("v", false, "verbose logging")
	flagInsecureTLS   = flag.Bool("k", camutil.InsecureTLS, "allow insecure TLS")
	flagSkipIrregular = flag.Bool("skip-irregular", camutil.SkipIrregular, "skip irregular files")
	//flagServer      = flag.String("server", ":3179", "Camlistore server address")
	flagCapCtime      = flag.Bool("capctime", false, "forge ctime to be less or equal to mtime")
	flagNoAuth        = flag.Bool("noauth", false, "no HTTP Basic Authentication, even if CAMLI_AUTH is set")
	flagListen        = flag.String("listen", ":3178", "listen on")
	flagParanoid      = flag.String("paranoid", "", "Paranoid mode: save uploaded files also under this dir")
	flagSkipHaveCache = flag.Bool("skiphavecache", false, "Skip have cache? (more stress on camlistored)")

	server string
)

func main() {
	Log := logger.Log

	client.AddFlags() // add -server flag
	flag.Parse()

	if *flagVerbose {
		camutil.Log = log.With(logger, "lib", "camutil").Log
	}

	server = client.ExplicitServer()
	camutil.Verbose = *flagVerbose
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
	Log("msg", "Listening", "http", s.Addr, "camlistore", server)
	if err := s.ListenAndServe(); err != nil {
		Log("msg", "finish", "error", err)
		os.Exit(1)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	Log := logger.Log

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
		dn, err := ioutil.TempDir("", "camproxy")
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot create temporary directory: %s", err), 500)
			return
		}
		var paraSource, paraDest string
		defer func() {
			if paraSource != "" && paraDest != "" { // save at last
				os.MkdirAll(filepath.Dir(paraDest), 0700)
				Log("msg", "Paranoid copying", "src", paraSource, "dst", paraDest)
				if err = camutil.LinkOrCopy(paraSource, paraDest); err != nil {
					Log("msg", "copying", "src", paraSource, "dst", paraDest, "error", err)
				}
			}
			os.RemoveAll(dn)
		}()

		var filenames, mimetypes []string

		ct := r.Header.Get("Content-Type")
		if ct, _, err = mime.ParseMediaType(ct); err != nil {
			Log("msg", "parsing Content-Type", "ct", ct, "error", err)
			if ct == "" {
				ct = r.Header.Get("Content-Type")
			}
		}
		Log("msg", "request Content-Type: "+ct)

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

		Log("msg", "uploading", "files", filenames, "mime-types", mimetypes)

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
	Log := logger.Log
	mimeType = r.Header.Get("Content-Type")
	lastmod := parseLastModified(r.Header.Get("Last-Modified"), r.URL.Query().Get("mtime"))
	cd := r.Header.Get("Content-Disposition")
	var fh *os.File
	fn := ""
	if cd != "" {
		_, params, err := mime.ParseMediaType(cd)
		if err != nil {
			Log("msg", "parsing Content-Disposition", "cd", cd, "error", err)
		} else {
			fn = params["filename"]
		}
	}
	if fn == "" {
		Log("msg", "Cannot determine filename", "content-disposition", cd)
		fh, err = ioutil.TempFile(destDir, "file-")
	} else {
		fn = filepath.Join(destDir, safeBaseFn(fn))
		fh, err = os.Create(fn)
	}
	if err != nil {
		return "", "", errors.Wrapf(err, "create temp file %q", fn)
	}
	defer fh.Close()
	rdr := io.Reader(r.Body)
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType, rdr = camutil.MIMETypeFromReader(r.Body)
	}
	_, err = io.Copy(fh, rdr)
	if err != nil {
		Log("msg", "saving request body", "dst", fh.Name(), "error", err)
	}
	filename = fh.Name()
	if !lastmod.IsZero() {
		if err = os.Chtimes(filename, lastmod, lastmod); err != nil {
			Log("msg", "chtimes", "dst", filename, "error", err)
		}
	}
	return
}

func saveMultipartTo(destDir string, mr *multipart.Reader, qmtime string) (filenames, mimetypes []string, err error) {
	Log := logger.Log

	var fn string
	var lastmod time.Time
	var part *multipart.Part
	for part, err = mr.NextPart(); err == nil; part, err = mr.NextPart() {
		filename := part.FileName()
		if filename == "" {
			if part.FormName() == "mtime" {
				b := bytes.NewBuffer(make([]byte, 23))
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
			return nil, nil, errors.Wrapf(err, "create temp file %q", fn)
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
			return nil, nil, errors.Wrapf(err, "write %q", fn)
		}
		lastmod = parseLastModified(part.Header.Get("Last-Modified"), qmtime)
		if !lastmod.IsZero() {
			if e := os.Chtimes(fn, lastmod, lastmod); e != nil {
				Log("msg", "chtimes", "dst", fn, "error", e)
			}
		}
	}
	return filenames, mimetypes, nil
}

func safeBaseFn(filename string) string {
	Log := logger.Log

	if i := strings.LastIndexAny(filename, "/\\"); i >= 0 {
		filename = filename[i+1:]
	}
	n := len(filename)
	for strings.IndexByte(filename, '%') >= 0 {
		if fn, err := url.QueryUnescape(filename); err != nil {
			Log("msg", "QueryUnescape", "name", filename, "error", err)
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
		Log("msg", "filename too long", "old", old, "new", filename)
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
	Log := logger.Log

	if len(mtimeHeader) >= 23 {
		if lastmod, ok = timeParse(mtimeHeader); ok {
			return lastmod
		}
		Log("msg", "too big an mtime "+mtimeHeader+", and not RFC1123-compliant")
		return lastmod
	}
	if qmt, err := strconv.ParseInt(mtimeHeader, 10, 64); err != nil {
		Log("msg", "cannot parse mtime", "header", mtimeHeader, "error", err)
	} else {
		return time.Unix(qmt, 0)
	}
	return lastmod
}

var mimeCache *camutil.MimeCache

type respWriter struct {
	http.ResponseWriter
	name, okMime  string
	headerWritten bool
	buf           []byte
}

func newRespWriter(w http.ResponseWriter, name, okMime string) *respWriter {
	if name != "" && (okMime == "" || okMime == "application/octet-stream") {
		m := mimeCache.Get(name)
		if m != "" {
			okMime = m
		}
	}
	return &respWriter{w, name, okMime, false, nil}
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
	return camutil.NewDownloader(server)
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
