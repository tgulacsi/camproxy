/*
Copyright 2020 Tamás Gulácsi

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

// Package percache is a Permanent Cache - a size bounded cache that combines
// perkeep's chunking and deduplication with ristretto's TinyLFU cache eviction
// policy for a size effective cache.
package percache

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	badgerdb "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/tgulacsi/camproxy/blobserver/badger"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
)

const (
	prefix      = ","
	valuePrefix = "/"

	DefaultMaxCount = 10_000
	DefaultMaxSize  = 1 << 30
)

func New(root string, maxCount, maxSize int64) (*PerCache, error) {
	if maxCount <= 0 {
		maxCount = DefaultMaxCount
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	pc := &PerCache{}
	var err error
	if pc.cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: maxCount * 10,
		MaxCost:     maxSize,
		BufferItems: 64,
		OnEvict:     pc.onCacheEvict,
	}); err != nil {
		return nil, err
	}
	bOpts := badgerdb.DefaultOptions(root)
	pc.db, err = badgerdb.Open(bOpts)
	if err != nil {
		os.RemoveAll(root)
		os.MkdirAll(root, 0755)
		if pc.db, err = badgerdb.Open(bOpts); err != nil {
			return nil, err
		}
	}
	var ctx context.Context
	ctx, pc.cancel = context.WithCancel(context.Background())
	go pc.db.Subscribe(ctx, pc.onDBEvent, []byte(valuePrefix))
	pc.sto = badger.NewManaged(pc.db, valuePrefix)
	return pc, nil
}

type PerCache struct {
	db     *badgerdb.DB
	cache  *ristretto.Cache
	sto    badger.Storage
	cancel func()
	mu     sync.Mutex
}

func (pc *PerCache) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cancel()
	pc.cache.Close()
	firstErr := pc.db.Close()
	if err := pc.sto.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
func (pc *PerCache) onCacheEvict(key uint64, value interface{}, cost int64) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	v, ok := value.([]byte)
	if !ok {
		return
	}
	pc.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(append(append(make([]byte, 0, len(valuePrefix)+len(v)), valuePrefix...), v...))
	})
}
func (pc *PerCache) onDBEvent(kvs *badgerdb.KVList) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, kv := range kvs.Kv {
		if len(kv.Key) <= len(valuePrefix) {
			continue
		}
		if len(kv.Value) == 0 {
			pc.cache.Del(kv.Key)
			continue
		}
		pc.cache.Set(bytes.TrimPrefix(kv.Key, []byte(valuePrefix)), kv.Key, int64(len(kv.Value)))
	}
	return nil
}

func (pc *PerCache) Get(ctx context.Context, nodeID string) (io.ReadCloser, error) {
	var br blob.Ref
	if err := pc.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(prefix + nodeID))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			return br.UnmarshalBinary(value)
		})
	}); err != nil {
		return nil, err
	}
	fr, err := schema.NewFileReader(ctx, pc.sto, br)
	if err == nil {
		fr.LoadAllChunks()
	}
	return fr, err
}

func (pc *PerCache) Put(ctx context.Context, nodeID string, data io.Reader) error {
	br, err := schema.WriteFileFromReader(ctx, pc.sto, nodeID, data)
	if err != nil {
		return err
	}
	return pc.db.Update(func(txn *badgerdb.Txn) error {
		b, _ := br.MarshalBinary()
		return txn.Set([]byte(prefix+nodeID), b)
	})
}
