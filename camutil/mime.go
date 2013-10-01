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
	"io"
	"log"

	"bitbucket.org/taruti/mimemagic"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/index/kvfile"
	"camlistore.org/pkg/lru"
)

// DefaultMaxMemMimeCacheSize is the maximum size of in-memory mime cache
var DefaultMaxMemMimeCacheSize = 1024

// MimeCache is the in-memory (LRU) and disk-based (kv) cache of mime types
type MimeCache struct {
	mem      *lru.Cache
	db       index.Storage
	dbcloser io.Closer
}

// NewMimeCache creates a new mime cache - in-memory + on-disk (persistent)
func NewMimeCache(filename string, maxMemCacheSize int) *MimeCache {
	mc := new(MimeCache)
	if maxMemCacheSize <= 0 {
		maxMemCacheSize = DefaultMaxMemMimeCacheSize
	}
	mc.mem = lru.New(maxMemCacheSize)

	var err error
	if mc.db, mc.dbcloser, err = kvfile.NewStorage(filename); err != nil {
		log.Printf("cannot open/create db %q: %s", filename, err)
		mc.db = nil
	}
	return mc
}

// Close closes the probably open disk db (kv)
func (mc *MimeCache) Close() error {
	if mc.dbcloser != nil {
		return mc.Close()
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
			log.Printf("error setting %q to %q in %s: %s", key, mime, mc.db, err)
		}
	}
}

// MatchMime checks mime from the first 1024 bytes
func MatchMime(guessMime string, data []byte) string {
	return mimemagic.Match(guessMime, data)
}
