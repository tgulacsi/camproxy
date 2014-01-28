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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/syncutil"
)

// Uploader holds the server and args
type Uploader struct {
	server string
	args   []string
	env    []string
	gate   *syncutil.Gate
	mtx    sync.Mutex
}

// FileIsEmpty is the error for zero length files
var FileIsEmpty = errors.New("File is empty")

var cachedUploader = make(map[string]*Uploader, 1)
var cachedUploaderMtx = new(sync.Mutex)

// Close closes the probably opened cached Uploaders and Downloaders
func Close() error {
	cachedUploaderMtx.Lock()
	defer cachedUploaderMtx.Unlock()
	for k := range cachedUploader {
		delete(cachedUploader, k)
	}
	cachedDownloaderMtx.Lock()
	defer cachedDownloaderMtx.Unlock()
	for k := range cachedDownloader {
		cachedDownloader[k].Close()
		delete(cachedDownloader, k)
	}
	return nil
}

// NewUploader returns a new uploader for uploading files to the given server
func NewUploader(server string, capCtime bool) *Uploader {
	cachedUploaderMtx.Lock()
	defer cachedUploaderMtx.Unlock()
	u, ok := cachedUploader[server]
	if ok {
		return u
	}
	u = &Uploader{server: server, args: []string{"file"}, gate: syncutil.NewGate(8)}
	if server != "" {
		u.args = []string{"-server=" + server, "file"}
	}
	if capCtime {
		u.args = append(u.args, "-capctime")
		if os.Getenv("CAMLI_DEBUG") != "true" { // -capctime needs CAMLI_DEBUG=true
			u.env = append(os.Environ(), "CAMLI_DEBUG=true")
		}
	}
	cachedUploader[server] = u
	return u
}

// UploadFile uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFile(path string, permanode bool) (content, perma blob.Ref, err error) {
	fh, err := os.Open(path)
	if err != nil {
		return
	}
	fi, err := fh.Stat()
	fh.Close()
	if err != nil {
		return
	}

	if fi.Size() <= 0 {
		err = FileIsEmpty
		return
	}
	u.gate.Start()
	defer u.gate.Done()
	i := len(u.args) + 2
	if permanode {
		i++
	}
	args := append(make([]string, 0, i), u.args...)
	if permanode {
		args = append(args, "--permanode")
	}
	var (
		rc   io.ReadCloser
		out  []byte
		blb  *schema.Blob
		down *Downloader
	)
	dir, base := filepath.Split(path)
	args = append(args, base)
	for i := 0; i < 10; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
		}
		log.Printf("camput %s", args)
		c := exec.Command("camput", args...)
		c.Dir = dir
		c.Env = u.env
		errbuf := bytes.NewBuffer(nil)
		c.Stderr = errbuf
		// serialize camput calls (have cache)
		func() {
			u.mtx.Lock()
			defer u.mtx.Unlock()
			out, err = c.Output()
		}()
		if err != nil {
			err = fmt.Errorf("error calling camput %q: %s (%s)", args, errbuf.Bytes(), err)
			return
		}
		var br, zbr blob.Ref
		var ok bool
		err = nil
		// the last line is the permanode ref, the first is the content
		for _, line := range bytes.Split(bytes.TrimSpace(out), []byte{'\n'}) {
			if br, ok = blob.Parse(string(line)); ok {
				if content == zbr {
					content = br
				} else {
					perma = br
				}
			}
		}
		if down == nil {
			if i > 0 {
				break
			}
			if down, err = NewDownloader(u.server); err != nil {
				log.Printf("cannot get downloader for checking uploads: %s", err)
				err = nil
				return
			}
		}
		if rc, err = fetch(down.dc, content); err == nil {
			blb, err = schema.BlobFromReader(content, rc)
			rc.Close()
			if err == nil {
				if len(blb.ByteParts()) > 0 {
					return
				}
				err = fmt.Errorf("blob[%s].parts is empty!", content)
				log.Println(err.Error() + "(" + blb.JSON() + ")")
			} else {
				log.Printf("error getting back blob %q: %s", content, err)
			}
		}
	}
	return
}

// RefToBase64 returns a base64-encoded version of the ref
func RefToBase64(br blob.Ref) string {
	data, err := br.MarshalBinary()
	if err != nil {
		log.Printf("error marshaling %v: %s", br, err)
		return ""
	}
	hn := br.HashName()
	return hn + "-" + base64.URLEncoding.EncodeToString(data[len(hn)+1:])
}
