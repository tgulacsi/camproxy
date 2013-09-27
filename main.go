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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"camlistore.org/pkg/client"
	"github.com/tgulacsi/camproxy/camutil"
)

var (
	flagVerbose     = flag.Bool("v", false, "verbose logging")
	flagInsecureTLS = flag.Bool("k", false, "allow insecure TLS")
	//flagServer      = flag.String("server", ":3147", "Camlistore server address")
	flagListen = flag.String("listen", ":3148", "listen on")

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
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Fatal(s.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		items, err := camutil.ParseBlobNames(nil, []string{r.URL.Path[1:]})
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		d, err := getDownloader()
		if err != nil {
			http.Error(w, fmt.Sprintf("error getting downloader to %q: %s",
				server, err), 500)
			return
		}
		if err = d.Download(&respWriter{w, false}, true, items...); err != nil {
			http.Error(w, fmt.Sprintf("error downloading %q: %s", items, err), 500)
			return
		}
		return

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
		switch len(files) {
		case 0:
			w.WriteHeader(404)
			w.Write(nil)
		case 1:
			fallthrough
		default:
			http.ServeFile(w, r, filepath.Join(dn, files[0]))
		}
	case "POST":
		u, err := getUploader()
		if err != nil {
			http.Error(w, fmt.Sprintf("error getting uploader to %q: %s", server, err), 500)
			return
		}
		_ = u
	default:
		http.Error(w, "Method must be GET/POST", 405)
	}
}

type respWriter struct {
	http.ResponseWriter
	headerWritten bool
}

func (w *respWriter) Write(p []byte) (int, error) {
	if !w.headerWritten {
		w.ResponseWriter.WriteHeader(200)
		w.headerWritten = true
	}
	return w.ResponseWriter.Write(p)
}

var cachedUploader *camutil.Uploader
var cachedDownloader *camutil.Downloader

func getUploader() (*camutil.Uploader, error) {
	if cachedUploader != nil {
		return cachedUploader, nil
	}
	u, err := camutil.NewUploader(server)
	if err != nil {
		return nil, err
	}
	cachedUploader = u
	return u, nil
}

func getDownloader() (*camutil.Downloader, error) {
	if cachedDownloader != nil {
		return cachedDownloader, nil
	}
	d, err := camutil.NewDownloader(server)
	if err != nil {
		return nil, err
	}
	cachedDownloader = d
	return d, nil
}
