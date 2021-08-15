// Copyright 2020, 2021 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

// Package percache is a Permanent Cache - a size bounded cache that combines
// perkeep's chunking and deduplication with ristretto's TinyLFU cache eviction
// policy for a size effective cache.
package percache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	badgerdb "github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto"
	"github.com/tgulacsi/camproxy/blobserver/badger"
	"github.com/tgulacsi/camproxy/blobserver/trace"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
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
		OnEvict: func(item *ristretto.Item) {
			if item == nil || item.Value == nil {
				return
			}
			pc.onCacheEvict(item.Key, item.Conflict, item.Value, item.Cost)
		},
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

	pc.sto = &trace.Storage{
		Storage: badger.NewManaged(pc.db, valuePrefix),
		OnFetch: func(blobs []blob.SizedRef, err error) {
			if err != nil {
				return
			}
			for _, br := range blobs {
				k, _ := br.Ref.MarshalBinary()
				pc.Log("msg", "fetch", "key", br.Ref.String())
				//log.Println("GET", k)
				pc.cache.Get(k)
			}
		},
		OnReceive: func(blobs []blob.SizedRef, err error) {
			if err != nil {
				return
			}
			//log.Println("SET", blobs)
			for _, br := range blobs {
				k, _ := br.Ref.MarshalBinary()
				pc.Log("msg", "receive", "key", br.Ref.String())
				pc.cache.Set(k, k, int64(br.Size))
			}
		},
		OnRemove: func(blobs []blob.SizedRef, err error) {
			if err != nil {
				return
			}
			for _, br := range blobs {
				k, _ := br.Ref.MarshalBinary()
				//log.Println("DEL", k)
				pc.Log("msg", "remove", "key", k)
				pc.cache.Del(k)
			}
		},
	}

	iOpts := badgerdb.DefaultIteratorOptions
	iOpts.PrefetchValues, iOpts.PrefetchSize, iOpts.Prefix = true, 16, []byte(valuePrefix)
	return pc, pc.db.View(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(iOpts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var ref blob.Ref
			if err := ref.UnmarshalBinary(bytes.TrimPrefix(item.Key(), []byte(valuePrefix))); err != nil {
				continue
			}
			if err := item.Value(func(val []byte) error {
				k, _ := ref.MarshalBinary()
				pc.Log("msg", "set cache from db", "key", k)
				pc.cache.Set(k, k, int64(len(val)))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

type PerCache struct {
	db      *badgerdb.DB
	cache   *ristretto.Cache
	sto     blobserver.Storage
	mu      sync.Mutex
	closing int32
	Logger
}
type Logger interface {
	Log(...interface{}) error
}

func (pc *PerCache) Log(keyvals ...interface{}) error {
	if pc.Logger != nil {
		if th, ok := pc.Logger.(interface{ Helper() }); ok {
			th.Helper()
		}
		return pc.Logger.Log(keyvals...)
	}
	return nil
}

func (pc *PerCache) Close() error {
	pc.mu.Lock()
	atomic.StoreInt32(&pc.closing, 1)
	defer func() {
		atomic.StoreInt32(&pc.closing, 0)
		pc.mu.Unlock()
	}()
	pc.cache.Close()
	firstErr := pc.db.Close()
	if cl, ok := pc.sto.(blobserver.ShutdownStorage); ok {
		if err := cl.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
func (pc *PerCache) onCacheEvict(key, conflict uint64, value interface{}, cost int64) {
	if atomic.LoadInt32(&pc.closing) == 0 {
		pc.mu.Lock()
		defer pc.mu.Unlock()
	}
	v, ok := value.([]byte)
	if !ok {
		return
	}
	//log.Println("EVICT", v)
	//pc.Log("msg", "EVICT", "v", string(v))

	pc.Log("msg", "evict", fmt.Sprintf("%x", value.([]byte)))
	pc.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(append(append(make([]byte, 0, len(valuePrefix)+len(v)), valuePrefix...), v...))
	})
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

func (pc *PerCache) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	return pc.sto.Fetch(ctx, br)
}
func (pc *PerCache) ReceiveBlob(ctx context.Context, br blob.Ref, r io.Reader) (blob.SizedRef, error) {
	return pc.sto.ReceiveBlob(ctx, br, r)
}
func (pc *PerCache) StatBlobs(ctx context.Context, br []blob.Ref, f func(blob.SizedRef) error) error {
	return pc.sto.StatBlobs(ctx, br, f)
}
