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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/localdisk"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/syncutil"
)

// Uploader holds the server and args
type Uploader struct {
	*client.Client
	server        string
	args          []string
	env           []string
	skipHaveCache bool
	gate          *syncutil.Gate
	mtx           sync.Mutex
	blobserver.StatReceiver
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
func NewUploader(server string, capCtime bool, skipHaveCache bool) *Uploader {
	cachedUploaderMtx.Lock()
	defer cachedUploaderMtx.Unlock()
	u, ok := cachedUploader[server]
	if ok {
		return u
	}
	if strings.HasPrefix(server, "file://") {
		recv, err := localdisk.New(server[7:])
		if err != nil {
			Log.Error("localdisk.New", "server", server, "error", err)
			return nil
		}
		u = &Uploader{server: server, gate: syncutil.NewGate(8), skipHaveCache: true,
			StatReceiver: recv}
		cachedUploader[server] = u
		return u
	}
	u = &Uploader{server: server, args: []string{"file"}, gate: syncutil.NewGate(8),
		skipHaveCache: skipHaveCache, Client: client.New(server)}
	u.StatReceiver = u.Client
	if server != "" {
		u.args = append([]string{"-server=" + server}, u.args...)
	}
	needDebugEnv := false
	if skipHaveCache {
		u.args = append([]string{"-havecache=false"}, u.args...)
		u.args = append(u.args, "-statcache=false")
		needDebugEnv = true
	}
	if capCtime {
		u.args = append(u.args, "-capctime")
		needDebugEnv = true
	}
	if needDebugEnv {
		if os.Getenv("CAMLI_DEBUG") != "true" { // -capctime needs CAMLI_DEBUG=true
			u.env = append(os.Environ(), "CAMLI_DEBUG=true")
		}
	}
	cachedUploader[server] = u
	return u
}

// FromReader uploads the contents of the io.Reader.
func (u *Uploader) FromReader(fileName string, r io.Reader) (blob.Ref, error) {
	Log.Debug("FromReader", "file", fileName)
	u.gate.Start()
	defer u.gate.Done()
	return schema.WriteFileFromReader(u.StatReceiver, filepath.Base(fileName), r)
}

// FromReaderInfo uploads the contents of r, wrapped with data from fi.
// Creation time (unixCtime) is capped at modification time (unixMtime), and
// a "mimeType" field is set, if mime is not empty.
func (u *Uploader) FromReaderInfo(fi os.FileInfo, mime string, r io.Reader) (blob.Ref, error) {
	Log.Debug("FromReaderInfo", "mime", mime)
	file := schema.NewCommonFileMap(filepath.Base(fi.Name()), fi)
	file = file.CapCreationTime().SetRawStringField("mimeType", mime)
	file = file.SetType("file")
	u.gate.Start()
	defer u.gate.Done()
	return schema.WriteFileMap(u.StatReceiver, file, r)
}

// UploadFile uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFile(
	path, mimeType string,
	permanode bool,
) (content, perma blob.Ref, err error) {
	direct := u.StatReceiver != nil
	if direct {
		fi, err := os.Stat(path)
		if err != nil {
			return content, perma, err
		}
		direct = fi.Mode().IsRegular()
	}
	if !direct {
		return u.UploadFileExt(path, permanode)
	}

	if content, err = u.UploadFileMIME(path, mimeType); !permanode || err != nil {
		return content, perma, err
	}
	pbRes, err := u.Client.UploadPlannedPermanode(content.String(), time.Now())
	if err != nil {
		return content, perma, err
	}
	perma = pbRes.BlobRef
	_, err = u.Client.UploadAndSignBlob(schema.NewAddAttributeClaim(pbRes.BlobRef, "camliContent", content.String()))

	return content, perma, err
}

// NewPermanode returns a new random permanode and sets the given attrs on it.
// Returns the permanode, and the error.
func (u *Uploader) NewPermanode(attrs map[string]string) (blob.Ref, error) {
	if u.Client != nil {
		pRes, err := u.Client.UploadNewPermanode()
		if err != nil {
			return blob.Ref{}, err
		}
		if len(attrs) > 0 {
			err = u.SetPermanodeAttributes(pRes.BlobRef, attrs)
		}
		return pRes.BlobRef, err
	}
	cmd := exec.Command("camput", "permanode")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return blob.Ref{}, err
	}
	br, ok := blob.ParseBytes(bytes.TrimSpace(out))
	if !ok {
		return br, fmt.Errorf("cannot parse %q as permanode blobref", out)
	}
	if len(attrs) > 0 {
		err = u.SetPermanodeAttributes(br, attrs)
	}
	return br, err
}

// SetPermanodeAttributes sets the attributes on the given permanode.
func (u *Uploader) SetPermanodeAttributes(perma blob.Ref, attrs map[string]string) error {
	if u.Client != nil {
		for k, v := range attrs {
			if _, err := u.Client.UploadAndSignBlob(schema.NewAddAttributeClaim(perma, k, v)); err != nil {
				return err
			}
		}
	} else {
		pS := perma.String()
		for k, v := range attrs {
			if err := exec.Command("camput", "attr", pS, k, v).Run(); err != nil {
				return err
			}
		}
	}
	return nil
}

// UploadFileMIME uploads a regular file with the given MIME type.
func (u *Uploader) UploadFileMIME(fileName, mimeType string) (content blob.Ref, err error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return content, err
	}
	defer fh.Close()
	fi, err := fh.Stat()
	if err != nil {
		return content, err
	}
	rdr := io.Reader(fh)
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType, rdr = MIMETypeFromReader(fh)
	}
	Log.Debug("direct upload with FromReader onto", "statreceiver", u.StatReceiver)
	br, err := u.FromReaderInfo(fi, mimeType, rdr)
	return br, err
}

// UploadFileExt uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFileExt(path string, permanode bool) (content, perma blob.Ref, err error) {
	Log.Info("UploadFile", "path", path, "permanode", permanode)
	fh, err := os.Open(path)
	if err != nil {
		return
	}
	defer fh.Close()
	fi, err := fh.Stat()
	if err != nil {
		return
	}

	if fi.Size() <= 0 {
		err = FileIsEmpty
		return
	}
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

	u.gate.Start()
	defer u.gate.Done()

	for i := 0; i < 10; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
		}
		Log.Info("camput", "args", args)
		c := exec.Command("camput", args...)
		c.Dir = dir
		c.Env = u.env
		errbuf := bytes.NewBuffer(nil)
		c.Stderr = errbuf
		if u.skipHaveCache {
			out, err = c.Output()
		} else {
			// serialize camput calls (have cache)
			func() {
				u.mtx.Lock()
				defer u.mtx.Unlock()
				out, err = c.Output()
			}()
		}
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
				Log.Error("cannot get downloader for checking uploads", "error", err)
				err = nil
				return
			}
		}
		if rc, err = fetch(down.Fetcher, content); err == nil {
			blb, err = schema.BlobFromReader(content, rc)
			rc.Close()
			if err != nil {
				Log.Error("error getting back blob", "blob", content, "error", err)
			} else {
				if len(blb.ByteParts()) > 0 {
					return
				}
				err = fmt.Errorf("blob[%s].parts is empty!", content)
				Log.Error(err.Error(), "blob", blb.JSON())
			}
		}
	}
	return
}

// RefToBase64 returns a base64-encoded version of the ref
func RefToBase64(br blob.Ref) string {
	data, err := br.MarshalBinary()
	if err != nil {
		Log.Error("error marshaling", "blob", br, "error", err)
		return ""
	}
	hn := br.HashName()
	return hn + "-" + base64.URLEncoding.EncodeToString(data[len(hn)+1:])
}
