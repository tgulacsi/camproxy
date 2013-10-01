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
	"fmt"
	"io"
	"log"
	"os/exec"
	"path/filepath"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/syncutil"
)

// Uploader holds the server and args
type Uploader struct {
	server string
	args   []string
	gate   *syncutil.Gate
}

// NewUploader returns a new uploader for uploading files to the given server
func NewUploader(server string) *Uploader {
	u := &Uploader{server: server, args: []string{"file"}, gate: syncutil.NewGate(8)}
	if server != "" {
		u.args = []string{"-server=" + server, "file"}
	}
	return u
}

// UploadFile uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFile(path string, permanode bool) (content, perma blob.Ref, err error) {
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
		log.Printf("camput %s", args)
		c := exec.Command("camput", args...)
		c.Dir = dir
		errbuf := bytes.NewBuffer(nil)
		c.Stderr = errbuf
		out, err = c.Output()
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
				log.Printf("blob[%s].parts is empty! (%s)", content, blb.JSON())
			} else {
				log.Printf("error getting back blob %q: %s", content, err)
			}
			time.Sleep(time.Duration(i) * time.Second)
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
