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
	"io"

	"github.com/golang/groupcache/lru"
	"gopkg.in/h2non/filetype.v1"
	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/sorted/kvfile"
)

// DefaultMaxMemMimeCacheSize is the maximum size of in-memory mime cache
var DefaultMaxMemMimeCacheSize = 1024

// MIMETypeFromReader takes a reader, sniffs the beginning of it,
// and returns the mime (if sniffed, else "") and a new reader
// that's the concatenation of the bytes sniffed and the remaining
// reader.
func MIMETypeFromReader(r io.Reader) (mime string, reader io.Reader) {
	if r == nil {
		return "", nil
	}
	var buf bytes.Buffer
	_, err := io.Copy(&buf, io.LimitReader(r, 1024))
	mt, _ := filetype.Match(buf.Bytes())
	mime = mt.MIME.Type + "/" + mt.MIME.Subtype
	if err != nil {
		return mime, io.MultiReader(bytes.NewReader(buf.Bytes()), errReader{err})
	}
	return mime, io.MultiReader(bytes.NewReader(buf.Bytes()), r)
}

// MimeCache is the in-memory (LRU) and disk-based (kv) cache of mime types
type MimeCache struct {
	mem *lru.Cache
	db  sorted.KeyValue
}

// NewMimeCache creates a new mime cache - in-memory + on-disk (persistent)
func NewMimeCache(filename string, maxMemCacheSize int) *MimeCache {
	mc := new(MimeCache)
	if maxMemCacheSize <= 0 {
		maxMemCacheSize = DefaultMaxMemMimeCacheSize
	}
	mc.mem = lru.New(maxMemCacheSize)

	var err error
	if mc.db, err = kvfile.NewStorage(filename); err != nil {
		Log("msg", "cannot open/create db", "file", filename, "error", err)
		mc.db = nil
	}
	return mc
}

// Close closes the probably open disk db (kv)
func (mc *MimeCache) Close() error {
	if mc.db != nil {
		return mc.db.Close()
	}
	return nil
}

// Get returns the stored mimetype for the key - empty string if not found
func (mc *MimeCache) Get(key string) string {
	if mti, ok := mc.mem.Get(key); ok {
		return mti.(string)
	}
	if mc.db != nil {
		if mimetype, err := mc.db.Get(key); err == nil {
			return mimetype
		}
	}
	return ""
}

// Set sets the mimetype for the key
func (mc *MimeCache) Set(key, mime string) {
	if mime == "" {
		return
	}
	mc.mem.Add(key, mime)
	if mc.db != nil {
		if err := mc.db.Set(key, mime); err != nil {
			Log("msg", "error setting", "key", key, "mime", mime, "db", mc.db, "error", err)
		}
	}
}

// MatchMime checks mime from the first 1024 bytes
func MatchMime(_ string, data []byte) string {
	mt, _ := filetype.Match(data)
	return mt.MIME.Type + "/" + mt.MIME.Subtype
}

type errReader struct {
	err error
}

func (er errReader) Read(_ []byte) (int, error) {
	return 0, er.err
}
