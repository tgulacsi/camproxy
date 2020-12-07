// Copyright 2020 The Perkeep Authors
//
// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2020 The Perkeep Authors

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

// Package limited implements the blobserver interface which limits the size of the underlying blobserver.Storage
package limited

import (
	"context"
	"io"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"

	"github.com/hashicorp/golang-lru/simplelru"
)

var _ = blobserver.Storage(Storage{})

// Storage is a size limited storage.
type Storage struct {
	storage     blobserver.Storage
	evictPolicy EvictPolicy
}

// NewStorage wraps the given storage, evicting using LRU.
func NewStorage(storage blobserver.Storage, size int) Storage {
	return NewStorageEvict(storage, LRUEvictPolicy(size))
}

// NewStorageEvict wraps the given storage, evicting using the given eviction policy.
//
// Only the new elements added to the underlying storage is limited!
func NewStorageEvict(storage blobserver.Storage, evictPolicy EvictPolicy) Storage {
	return Storage{storage: storage, evictPolicy: evictPolicy}
}

// EvictPolicy represents a cache eviction policy returns what has been evicted.
type EvictPolicy func(addKey string) (evicted string)

// LRUEvictPolicy returns a simple LRU based eviction policy.
func LRUEvictPolicy(size int) EvictPolicy {
	var evicted string
	lru, err := simplelru.NewLRU(size, func(key, value interface{}) { evicted = key.(string) })
	if err != nil {
		panic(err)
	}
	var null struct{}
	return func(key string) string {
		if !lru.Add(key, null) {
			return ""
		}
		return evicted
	}
}

func (sto Storage) touch(ctx context.Context, br blob.Ref) {
	if evict := sto.evictPolicy(br.String()); evict != "" {
		if ebr, ok := blob.Parse(evict); ok && ebr.Valid() {
			_ = sto.storage.RemoveBlobs(ctx, []blob.Ref{ebr})
		}
	}
}

func (sto Storage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	sto.touch(ctx, br)
	return sto.storage.Fetch(ctx, br)
}

func (sto Storage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	return sto.storage.RemoveBlobs(ctx, blobs)
}

func (sto Storage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	err := sto.storage.StatBlobs(ctx, blobs, func(sr blob.SizedRef) error {
		if err := fn(sr); err != nil {
			return err
		}
		sto.touch(ctx, sr.Ref)
		return nil
	})
	return err
}

func (sto Storage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	ch := make(chan blob.SizedRef)
	go func() {
		defer close(dest)
		for sr := range ch {
			dest <- sr
			sto.touch(ctx, sr.Ref)
		}
	}()
	return sto.storage.EnumerateBlobs(ctx, ch, after, limit)
}

func (sto Storage) ReceiveBlob(ctx context.Context, br blob.Ref, source io.Reader) (blob.SizedRef, error) {
	sr, err := sto.storage.ReceiveBlob(ctx, br, source)
	if err != nil {
		return sr, err
	}
	sto.touch(ctx, br)
	return sr, nil
}

var _ = blobserver.ShutdownStorage((*Storage)(nil))

func (sto Storage) Close() error {
	if cl, ok := sto.storage.(blobserver.ShutdownStorage); ok {
		return cl.Close()
	}
	return nil
}

var _ = blobserver.Generationer((*Storage)(nil))

func (sto Storage) StorageGeneration() (initTime time.Time, random string, err error) {
	if sg, ok := sto.storage.(blobserver.Generationer); ok {
		return sg.StorageGeneration()
	}
	err = blobserver.ErrNotImplemented
	return
}

func (sto Storage) ResetStorageGeneration() error {
	if sg, ok := sto.storage.(blobserver.Generationer); ok {
		return sg.ResetStorageGeneration()
	}
	return blobserver.ErrNotImplemented
}
