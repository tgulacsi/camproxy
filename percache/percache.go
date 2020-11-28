// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

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

	pc.sto = &trace.Storage{
		Storage: badger.NewManaged(pc.db, valuePrefix),
		OnFetch: func(blobs []blob.SizedRef, err error) {
			if err != nil {
				return
			}
			for _, br := range blobs {
				k, _ := br.Ref.MarshalBinary()
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
	db    *badgerdb.DB
	cache *ristretto.Cache
	sto   blobserver.Storage
	mu    sync.Mutex
}

func (pc *PerCache) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cache.Close()
	firstErr := pc.db.Close()
	if cl, ok := pc.sto.(blobserver.ShutdownStorage); ok {
		if err := cl.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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
	//log.Println("EVICT", v)
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
