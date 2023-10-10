// Copyright 2013, 2023 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"bytes"
	"io"

	lru "github.com/hashicorp/golang-lru"
	"github.com/zRedShift/mimemagic"
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
	var name string
	if namer, ok := r.(interface{ Name() string }); ok {
		name = namer.Name()
	}
	mime = mimemagic.Match(buf.Bytes(), name).MediaType()
	if err != nil {
		return mime, io.MultiReader(bytes.NewReader(buf.Bytes()), errReader{err})
	}
	return mime, io.MultiReader(bytes.NewReader(buf.Bytes()), r)
}

// MimeCache is the in-memory (LRU) and disk-based (kv) cache of mime types
type MimeCache struct {
	mem *lru.TwoQueueCache
	db  sorted.KeyValue
}

// NewMimeCache creates a new mime cache - in-memory + on-disk (persistent)
func NewMimeCache(filename string, maxMemCacheSize int) *MimeCache {
	if maxMemCacheSize <= 0 {
		maxMemCacheSize = DefaultMaxMemMimeCacheSize
	}
	mem, err := lru.New2Q(maxMemCacheSize)
	if err != nil {
		panic(err)
	}

	db, err := kvfile.NewStorage(filename)
	if err != nil {
		logger.Error("open/create db", "file", filename, "error", err)
	}
	return &MimeCache{mem: mem, db: db}
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
			logger.Error("setting", "key", key, "mime", mime, "db", mc.db, "error", err)
		}
	}
}

// MatchMime checks mime from the first 1024 bytes
func MatchMime(_ string, data []byte) string {
	return mimemagic.Match(data, "").MediaType()
}

type errReader struct {
	err error
}

func (er errReader) Read(_ []byte) (int, error) {
	return 0, er.err
}
