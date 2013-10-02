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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/client"

	"github.com/tgulacsi/camproxy/camutil"
)

var (
	flagVerbose     = flag.Bool("v", false, "verbose logging")
	flagInsecureTLS = flag.Bool("k", false, "allow insecure TLS")
	//flagServer      = flag.String("server", ":3179", "Camlistore server address")
	flagNoAuth   = flag.Bool("noauth", false, "no HTTP Basic Authentication, even if CAMLI_AUTH is set")
	flagListen   = flag.String("listen", ":3178", "listen on")
	flagParanoid = flag.String("paranoid", "", "Paranoid mode: save uploaded files also under this dir")

	server string
)

func main() {
	client.AddFlags() // add -server flag
	flag.Parse()
	server = client.ExplicitServer()
	camutil.Verbose = *flagVerbose
	camutil.InsecureTLS = *flagInsecureTLS
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
	mimeCache = camutil.NewMimeCache(filepath.Join(os.TempDir(), "mimecache.kv"), 0)
	defer mimeCache.Close()
	log.Printf("listening on %q using Camlistore server at %q", s.Addr, server)
	log.Fatal(s.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "empty request", 400)
		return
	}
	if r.Body != nil {
		defer r.Body.Close()
	}

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
		d, err := getDownloader()
		if err != nil {
			http.Error(w, fmt.Sprintf("error getting downloader to %q: %s",
				server, err), 500)
			return
		}
		content := r.URL.Query().Get("raw") != "1"
		okMime, nm := "", ""
		if !content {
			okMime = "application/json"
		} else {
			okMime = r.URL.Query().Get("mimeType")
			if 1 == len(items) {
				nm = camutil.RefToBase64(items[0])
				if okMime == "" {
					okMime = mimeCache.Get(nm)
				}
			}
		}
		// TODO(gt): retrieve proper mime-type
		rw := newRespWriter(w, nm, okMime)
		defer rw.Close()
		if err = d.Download(rw, content, items...); err != nil {
			http.Error(w, fmt.Sprintf("error downloading %q: %s", items, err), 500)
			return
		}
		return

		/*
			dn, err := ioutil.TempDir("", "camli")
			if err != nil {
				http.Error(w, "error creating temp file for download: "+err.Error(), 500)
				return
			}
			defer os.RemoveAll(dn)
			if err = d.Save(dn, true, items...); err != nil {
				http.Error(w, fmt.Sprintf("error downloading %q: %s", items, err), 500)
				return
			}
			dh, err := os.Open(dn)
			if err != nil {
				http.Error(w, fmt.Sprintf("cannot open temp dir %q: %s", dn, err), 500)
				return
			}
			defer dh.Close()
			files, err := dh.Readdirnames(-1)
			if err != nil {
				http.Error(w, fmt.Sprintf("error listing temp dir %q: %s", dn, err), 500)
				return
			}
			accept := r.Header.Get("Accept")
			log.Printf("Accept=%q", accept)
			switch len(files) {
			case 0:
				w.WriteHeader(404)
				w.Write(nil)
			case 1:
				if accept == "application/tar" || accept == "application/zip" {
					externalArchDir(&respWriter{w, accept, false}, dn, accept[12:])
				} else {
					http.ServeFile(w, r, filepath.Join(dn, files[0]))
				}
			default:
				switch accept {
				case "application/x-tar", "application/tar":
					tarDir(&respWriter{w, "application/tar", false}, dn)
				default:
					zipDir(&respWriter{w, "application/zip", false}, dn)
				}
			}
		*/
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
				log.Printf("Paranoid copying %q to %q", paraSource, paraDest)
				if err = camutil.LinkOrCopy(paraSource, paraDest); err != nil {
					log.Printf("error copying %q to %q: %s", paraSource, paraDest, err)
				}
			}
			os.RemoveAll(dn)
		}()

		var filenames, mimetypes []string

		ct := r.Header.Get("Content-Type")
		if ct, _, err = mime.ParseMediaType(ct); err != nil {
			log.Printf("error parsing Content-Type %q: %s", ct, err)
			if ct == "" {
				ct = r.Header.Get("Content-Type")
			}
		}
		log.Printf("Content-Type: %q", ct)

		switch ct {
		case "multipart/form", "multipart/form-data", "application/x-www-form-urlencoded":
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, fmt.Sprintf("error parsing request body as multipart/form: %s", err), 400)
				return
			}
			filenames, mimetypes, err = saveMultipartTo(dn, mr, r.URL.Query().Get("mtime"))
		default: // legacy direct upload
			var fn, mime string
			fn, mime, err = saveDirectTo(dn, r)
			//log.Printf("direct: %s", err)
			if fn != "" {
				filenames = append(filenames, fn)
				mimetypes = append(mimetypes, mime)
			}
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		log.Printf("uploading %q %q", filenames, mimetypes)

		permanode := r.URL.Query().Get("permanode") == "1"
		short := r.URL.Query().Get("short") == "1"
		var content, perma blob.Ref
		switch len(filenames) {
		case 0:
			http.Error(w, "no files in request", 400)
			return
		case 1:
			content, perma, err = u.UploadFile(filenames[0], permanode)
		default:
			content, perma, err = u.UploadFile(dn, permanode)
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
		http.Error(w, "Method must be GET/POST", 405)
	}
}

func saveDirectTo(destDir string, r *http.Request) (filename, mimetype string, err error) {
	mimetype = r.Header.Get("Content-Type")
	log.Printf("headers: %q", r.Header)
	cd := r.Header.Get("Content-Disposition")
	var fh *os.File
	fn := ""
	if cd != "" {
		_, params, err := mime.ParseMediaType(cd)
		if err != nil {
			log.Printf("error parsing Content-Disposition %q: %s", cd, err)
		} else {
			fn = params["filename"]
		}
	}
	//log.Printf("creating file %q in %q", fn, destDir)
	if fn == "" {
		log.Printf("cannot determine filename from %q", cd)
		fh, err = ioutil.TempFile(destDir, "file-")
	} else {
		//log.Printf("fn=%q", fn)
		fn = filepath.Join(destDir, filepath.Base(fn))
		fh, err = os.Create(fn)
	}
	if err != nil {
		return "", "", fmt.Errorf("error creating temp file %q: %s", fn, err)
	}
	defer fh.Close()
	//log.Printf("saving request body to %q...", fh.Name())
	_, err = io.Copy(fh, r.Body)
	if err != nil {
		log.Printf("saving request body to %q: %s", fh.Name(), err)
	}
	filename = fh.Name()
	return
}

func saveMultipartTo(destDir string, mr *multipart.Reader, qmtime string) (filenames, mimetypes []string, err error) {
	var fn string
	var qmt int64
	for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
		defer part.Close()
		filename := part.FileName()
		if filename == "" {
			if qmtime == "" && part.FormName() == "mtime" {
				b := bytes.NewBuffer(make([]byte, 23))
				if n, err := io.CopyN(b, part, 23); err == nil || err == io.EOF {
					err = nil
					if n >= 23 {
						log.Printf("too big an mtime %q", b)
					} else {
						qmtime = b.String()
					}
				}
			}
			continue
		}
		fn = filepath.Join(destDir, filepath.Base(filename))
		fh, err := os.Create(fn)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating temp file %q: %s", fn, err)
		}
		if _, err = io.Copy(fh, part); err == nil {
			if qmt == 0 && qmtime != "" {
				if qmt, err = strconv.ParseInt(qmtime, 10, 64); err != nil {
					log.Printf("cannot parse mtime %q: %s", qmtime, err)
					err, qmt, qmtime = nil, 0, ""
				}
			}
			filenames = append(filenames, fh.Name())
			mimetypes = append(mimetypes, part.Header.Get("Content-Type"))
		}
		fh.Close()
		log.Printf("qmt=%d", qmt)
		if err == nil && qmt > 0 {
			t := time.Unix(qmt, 0)
			if err = os.Chtimes(fn, t, t); err != nil {
				log.Printf("error chtimes %q: %s", fn, err)
			}
		}
		if err != nil {
			return nil, nil, fmt.Errorf("error writing to %q: %s", fn, err)
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
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
	return camutil.NewUploader(server), nil
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
