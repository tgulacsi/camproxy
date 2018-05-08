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
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go4.org/syncutil"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/localdisk"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"
)

// Uploader holds the server and args
type Uploader struct {
	*client.Client
	server        string
	args          []string
	opts          []string
	env           []string
	skipHaveCache bool
	gate          *syncutil.Gate
	mtx           sync.Mutex
	blobserver.StatReceiver
	*schema.Signer
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
			Log("msg", "localdisk.New", "server", server, "error", err)
			return nil
		}
		u = &Uploader{
			server:        server,
			gate:          syncutil.NewGate(8),
			skipHaveCache: true,
			StatReceiver:  recv,
			Signer:        newDummySigner(),
		}
		cachedUploader[server] = u
		return u
	}
	c, err := NewClient(server)
	if err != nil || c == nil {
		Log("msg", "NewClient", "server", server, "error", err)
		return nil
	}
	u = &Uploader{
		server:        server,
		args:          make([]string, 1, 2),
		opts:          make([]string, 0, 3),
		gate:          syncutil.NewGate(32),
		skipHaveCache: skipHaveCache,
		Client:        c,
		StatReceiver:  c,
	}
	u.args[0] = cmdPkPut
	if server != "" {
		u.args = append(u.args, "-server="+server)
	}
	needDebugEnv := false
	if skipHaveCache {
		u.opts = append(u.opts, "-havecache=false", "-statcache=false")
		needDebugEnv = true
	}
	if capCtime {
		u.opts = append(u.opts, "-capctime")
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

// Close closes the Client/Storage.
func (u *Uploader) Close() error {
	var err error
	if u.StatReceiver != nil {
		if cl, ok := u.StatReceiver.(io.Closer); ok {
			err = cl.Close()
		}
		u.StatReceiver = nil
	}
	if u.Client == nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	err = u.Client.Close()
	return err
}

// FromReader uploads the contents of the io.Reader.
func (u *Uploader) FromReader(ctx context.Context, fileName string, r io.Reader) (blob.Ref, error) {
	u.gate.Start()
	defer u.gate.Done()
	return schema.WriteFileFromReader(ctx, u.StatReceiver, filepath.Base(fileName), r)
}

// FromReaderInfo uploads the contents of r, wrapped with data from fi.
// Creation time (unixCtime) is capped at modification time (unixMtime), and
// a "mimeType" field is set, if mime is not empty.
func (u *Uploader) FromReaderInfo(ctx context.Context, fi os.FileInfo, mime string, r io.Reader) (blob.Ref, error) {
	file := schema.NewCommonFileMap(filepath.Base(fi.Name()), fi)
	file = file.CapCreationTime().SetRawStringField("mimeType", mime)
	file = file.SetType("file")
	u.gate.Start()
	defer u.gate.Done()
	return schema.WriteFileMap(ctx, u.StatReceiver, file, r)
}

// UploadFile uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFile(
	ctx context.Context,
	path, mime string,
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
		return u.UploadFileExt(ctx, path, permanode)
	}

	if content, err = u.UploadFileMIME(ctx, path, mime); !permanode || err != nil {
		return content, perma, err
	}
	pbRes, err := u.Client.UploadPlannedPermanode(ctx, content.String(), time.Now())
	if err != nil {
		return content, perma, err
	}
	perma = pbRes.BlobRef
	_, err = u.Client.UploadAndSignBlob(ctx, schema.NewAddAttributeClaim(pbRes.BlobRef, "camliContent", content.String()))

	return content, perma, err
}

// UploadFileLazyAttr uploads the given path (file or directory, recursively), and
// returns the content ref, and the permanode ref iff attrs is not empty.
// It also sets the attributes on the permanode - but only those without "camli" prefix!
//
// This is lazy, so it will NOT return an error if the permanode/attrs can't be created.
func (u *Uploader) UploadFileLazyAttr(
	ctx context.Context,
	path, mime string,
	attrs map[string]string,
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
		return u.UploadFileExtLazyAttr(ctx, path, attrs)
	}

	filteredAttrs := filterAttrs("camli", attrs)
	if content, err = u.UploadFileMIME(ctx, path, mime); len(filteredAttrs) == 0 || err != nil {
		return content, perma, err
	}

	filteredAttrs["camliContent"] = content.String()
	if perma, err = u.NewPermanode(ctx, filteredAttrs); err != nil {
		Log("msg", "NewPermanode", "attrs", filteredAttrs, "error", err)
	}
	return content, perma, nil
}

// UploadReaderLazyAttr uploads the contents of the reader as a file,
// returns the content ref, and the permanode ref iff attrs is not empty.
// It also sets the attributes on the permanode - but only those without "camli" prefix!
//
// This is lazy, so it will NOT return an error if the permanode/attrs can't be created.
func (u *Uploader) UploadReaderInfoLazyAttr(
	ctx context.Context,
	fi os.FileInfo, mime string, r io.Reader,
	attrs map[string]string,
) (content, perma blob.Ref, err error) {
	filteredAttrs := filterAttrs("camli", attrs)
	if content, err = u.FromReaderInfo(ctx, fi, mime, r); err != nil || len(filteredAttrs) == 0 {
		return content, perma, err
	}
	filteredAttrs["camliContent"] = content.String()
	if perma, err = u.NewPermanode(ctx, filteredAttrs); err != nil {
		Log("msg", "NewPermanode", "attrs", filteredAttrs, "error", err)
	}
	return content, perma, nil
}

func filterAttrs(skipPrefix string, attrs map[string]string) map[string]string {
	filteredAttrs := make(map[string]string, len(attrs)+1)
	for k, v := range attrs {
		if strings.HasPrefix(k, skipPrefix) {
			continue
		}
		filteredAttrs[k] = v
	}
	return filteredAttrs
}

// NewPermanode returns a new random permanode and sets the given attrs on it.
// Returns the permanode, and the error.
func (u *Uploader) NewPermanode(ctx context.Context, attrs map[string]string) (blob.Ref, error) {
	if u.Client != nil {
		pRes, err := u.Client.UploadNewPermanode(ctx)
		if err != nil {
			Log("msg", "UploadNewPermanode", "error", err)
			return blob.Ref{}, err
		}
		if len(attrs) > 0 {
			err = u.SetPermanodeAttrs(ctx, pRes.BlobRef, attrs)
		}
		return pRes.BlobRef, err
	}
	if u.Signer != nil {
		signed, err := schema.NewUnsignedPermanode().Sign(ctx, u.Signer)
		if err != nil {
			Log("msg", "Sign", "signer", u.Signer, "error", err)
			return blob.Ref{}, err
		}
		return blob.RefFromString(signed), err
	}
	refs, err := u.camput(ctx, "permanode")
	if err != nil || len(refs) == 0 {
		return blob.Ref{}, err
	}
	err = u.SetPermanodeAttrs(ctx, refs[0], attrs)
	return refs[0], err
}

// SetPermanodeAttrs sets the attributes on the given permanode.
func (u *Uploader) SetPermanodeAttrs(ctx context.Context, perma blob.Ref, attrs map[string]string) error {
	var setAttr func(k, v string) (blob.Ref, error)
	if u.Client != nil {
		setAttr = func(k, v string) (blob.Ref, error) {
			pRes, err := u.Client.UploadAndSignBlob(ctx, schema.NewSetAttributeClaim(perma, k, v))
			if err != nil {
				return blob.Ref{}, err
			}
			return pRes.BlobRef, nil
		}
	} else {
		pS := perma.String()
		setAttr = func(k, v string) (blob.Ref, error) {
			refs, err := u.camput(ctx, "attr", pS, k, v)
			if err != nil || len(refs) == 0 {
				return blob.Ref{}, err
			}
			return refs[0], nil
		}
	}
	for k, v := range attrs {
		if _, err := setAttr(k, v); err != nil {
			Log("msg", "SetPermanodeAttrs", "key", k, "value", v, "perma", perma.String(), "error", err)
			return err
		}
	}
	return nil
}

// UploadFileMIME uploads a regular file with the given MIME type.
func (u *Uploader) UploadFileMIME(ctx context.Context, fileName, mimeType string) (content blob.Ref, err error) {
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
	br, err := u.FromReaderInfo(ctx, fi, mimeType, rdr)
	return br, err
}

// UploadFileExt uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (if you asked for it), and error
func (u *Uploader) UploadFileExt(ctx context.Context, path string, permanode bool) (content, perma blob.Ref, err error) {
	Log("msg", "UploadFileExt", "path", path, "permanode", permanode)
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
	args := make([]string, 1, 2)
	args[0] = path
	if permanode {
		args = append(args, "--permanode")
	}
	refs, err := u.camput(ctx, "file", args...)
	if len(refs) > 0 {
		content = refs[0]
		if len(refs) > 1 {
			perma = refs[1]
		}
	}
	return content, perma, err
}

// UploadFileExtLazyAttr uploads the given path (file or directory, recursively), and
// returns the content ref, the permanode ref (iff you added attributes).
func (u *Uploader) UploadFileExtLazyAttr(ctx context.Context, path string, attrs map[string]string) (content, perma blob.Ref, err error) {
	Log("msg", "UploadFileExtLazyAttr", "path", path, "attrs", attrs)
	filteredAttrs := filterAttrs("camli", attrs)
	content, perma, err = u.UploadFileExt(ctx, path, len(filteredAttrs) > 0)
	if perma.Valid() {
		if err := u.SetPermanodeAttrs(ctx, perma, filteredAttrs); err != nil {
			Log("msg", "SetPermanodeAttrs", "perma", perma.String(), "attrs", filteredAttrs, "error", err)
		}
	}
	return content, perma, err
}

func (u *Uploader) camput(ctx context.Context, mode string, modeArgs ...string) ([]blob.Ref, error) {
	args := make([]string, 0, len(u.args)+1+len(u.opts)+len(modeArgs)+1)
	args = append(append(append(args, u.args...), mode), u.opts...)
	var dir string
	if mode == "file" {
		var base string
		dir, base = filepath.Split(modeArgs[0])
		args = append(args, base)
	}

	refs := make([]blob.Ref, 0, 2)

	u.gate.Start()
	defer u.gate.Done()

	var (
		lastErr error
		errbuf  bytes.Buffer
		down    *Downloader
	)

	for i := 0; i < 10; i++ {
		if i > 0 {
			errbuf.Reset()
			time.Sleep(time.Duration(i) * time.Second)
		}
		Log("msg", cmdPkPut, "args", args)
		c := exec.Command(cmdPkPut, args[0:]...)
		c.Dir = dir
		c.Env = u.env
		c.Stderr = &errbuf

		if !u.skipHaveCache {
			// serialize camput calls (have cache)
			u.mtx.Lock()
		}
		out, err := c.Output()
		if !u.skipHaveCache {
			u.mtx.Unlock()
		}
		if err != nil {
			lastErr = errors.Wrapf(err, "call %s %q: %s", cmdPkPut, args, errbuf.Bytes())
			continue
		}
		// the last line is the permanode ref, the first is the content
		for _, line := range bytes.Split(out, []byte{'\n'}) {
			if line = bytes.TrimSpace(line); len(line) == 0 {
				continue
			}
			if br, ok := blob.Parse(string(line)); ok {
				if br.Valid() {
					refs = append(refs, br)
				}
			}
		}
		if len(refs) == 0 {
			break
		}
		if down == nil {
			if i > 0 {
				break
			}
			if down, err = NewDownloader(u.server); err != nil {
				Log("msg", "cannot get downloader for checking uploads", "error", err)
				break
			}
		}
		content := refs[0]
		if rc, err := fetch(ctx, down.Fetcher, content); err == nil {
			blb, err := schema.BlobFromReader(content, rc)
			rc.Close()
			if err != nil {
				Log("msg", "error getting back blob", "blob", content, "error", err)
			} else {
				if len(blb.ByteParts()) > 0 {
					break
				}
				lastErr = errors.New(fmt.Sprintf("blob[%s].parts is empty!", content))
				Log("msg", "blob", blb.JSON())
			}
		}
	}
	return refs, lastErr
}

// RefToBase64 returns a base64-encoded version of the ref
func RefToBase64(br blob.Ref) string {
	if !br.Valid() {
		return ""
	}
	data, err := br.MarshalBinary()
	if err != nil {
		Log("msg", "error marshaling", "blob", br, "error", err)
		return ""
	}
	hn := br.HashName()
	return hn + "-" + base64.URLEncoding.EncodeToString(data[len(hn)+1:])
}

func newDummySigner() *schema.Signer {
	var privateKeySource *openpgp.Entity
	for _, fn := range []string{
		"$HOME/.config/camlistore/identity-secring.gpg",
		"$HOME/.gnupg/secring.gpg",
	} {
		fh, err := os.Open(os.ExpandEnv(fn))
		if err != nil {
			Log("msg", "open", "file", fn, "error", err)
			continue
		}
		el, err := openpgp.ReadKeyRing(fh)
		fh.Close()
		if err != nil {
			Log("msg", "ReadKeyRing", "file", fh.Name(), "error", err)
			continue
		}
		for _, e := range el {
			if e.PrivateKey == nil {
				continue
			}
			privateKeySource = e
			break
		}
		if privateKeySource != nil {
			break
		}
	}
	if privateKeySource == nil {
		var err error
		if privateKeySource, err = openpgp.NewEntity(
			"camutil", "test", "camutil@camlistore.org", nil,
		); err != nil {
			Log("msg", "openpgp.NewEntity", "error", err)
			return nil
		}
	}
	var buf bytes.Buffer
	hsh := blob.RefFromString("").Hash()
	w, err := armor.Encode(io.MultiWriter(&buf, hsh), "PGP PUBLIC KEY BLOCK", nil)
	if err != nil {
		Log("msg", "armor", "error", err)
		return nil
	}
	if err = privateKeySource.PrimaryKey.Serialize(w); err != nil {
		Log("msg", "serialize", "error", err)
	}
	_ = w.Close()

	pubKeyRef := blob.RefFromHash(hsh)
	armoredPubKey := bytes.NewReader(buf.Bytes())

	signer, err := schema.NewSigner(pubKeyRef, armoredPubKey, privateKeySource)
	if err != nil {
		Log("msg", "newDummySigner", "pubkey", pubKeyRef, "pubkey", armoredPubKey, "privatekey", privateKeySource, "error", err)
		return nil
	}
	return signer
}

var cmdPkPut = "pk-put"

func init() {
	if _, err := exec.LookPath("pk-put"); err != nil {
		if _, err = exec.LookPath("camput"); err == nil {
			cmdPkPut = "camput"
		}
	}
}

// vim: fileencoding=utf-8:
