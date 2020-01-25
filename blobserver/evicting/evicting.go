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

/*
Package evicting implements the blobserver interface by storing each blob
in its underlying blob store - but as a size limited "cache",
using TinyLFU evicting policy (what ristretto uses).
*/
package evicting

import (
	"context"
	"io"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"

	"github.com/dgraph-io/ristretto"

	"golang.org/x/sync/errgroup"
)

var _ = blobserver.Storage((*Storage)(nil))

type Storage struct {
	bs    blobserver.Storage
	cache *ristretto.Cache
}

func NewStorage(bs blobserver.Storage, maxItemCount, maxSize int64) *Storage {
	ds := &Storage{bs: bs}
	var err error
	ds.cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * maxItemCount,
		MaxCost:     maxSize,
		BufferItems: 64,
		OnEvict:     ds.evict,
	})
	if err != nil {
		panic(err)
	}
	ch := make(chan blob.SizedRef)
	go func() {
		for x := range ch {
			key := x.Ref.String()
			ds.cache.Set(key, key, int64(x.Size))
		}
	}()
	go ds.bs.EnumerateBlobs(context.Background(), ch, "", int(maxItemCount))
	return ds
}

func (ds *Storage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	key := br.String()
	_, exist := ds.cache.Get(key)
	rc, size, err := ds.bs.Fetch(ctx, br)
	if err != nil {
		if exist {
			ds.cache.Del(key)
		}
		return rc, size, err
	}
	if !exist {
		ds.cache.Set(br.String(), br.String(), int64(size))
	}
	return rc, size, err
}

// evict is called for every eviction and passes the hashed key, value,
// and cost to the function.
func (ds *Storage) evict(key, conflict uint64, value interface{}, cost int64) {
	s, ok := value.(string)
	if !ok {
		return
	}
	br := blob.ParseOrZero(s)
	if br.Valid() {
		ds.bs.RemoveBlobs(context.Background(), []blob.Ref{br})
	}
}

func (ds *Storage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	for _, blob := range blobs {
		ds.cache.Del(blob.String())
	}
	return ds.bs.RemoveBlobs(ctx, blobs)
}

func (ds *Storage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	for _, blob := range blobs {
		ds.cache.Get(blob.String())
	}
	return ds.bs.StatBlobs(ctx, blobs, fn)
}

func (ds *Storage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	grp, grpCtx := errgroup.WithContext(ctx)
	ch := make(chan blob.SizedRef)
	grp.Go(func() error { return ds.bs.EnumerateBlobs(grpCtx, ch, after, limit) })
	grp.Go(func() error {
		for {
			select {
			case <-grpCtx.Done():
				return grpCtx.Err()
			case x, ok := <-ch:
				if !ok {
					return nil
				}
				key := x.Ref.String()
				if _, ok := ds.cache.Get(key); !ok {
					ds.cache.Set(key, key, int64(x.Size))
				}
				dest <- x
			}
		}
		return nil
	})
	return grp.Wait()
}

var _ = blobserver.Generationer((*Storage)(nil))

func (ds *Storage) StorageGeneration() (initTime time.Time, random string, err error) {
	if sg, ok := ds.bs.(blobserver.Generationer); ok {
		return sg.StorageGeneration()
	}
	return time.Time{}, "", blobserver.GenerationNotSupportedError("underlying storage does not support StorageGeneration")
}

func (ds *Storage) ResetStorageGeneration() error {
	if rg, ok := ds.bs.(blobserver.Generationer); ok {
		return rg.ResetStorageGeneration()
	}
	return nil
}

func (ds *Storage) ReceiveBlob(ctx context.Context, br blob.Ref, source io.Reader) (blob.SizedRef, error) {
	sr, err := ds.bs.ReceiveBlob(ctx, br, source)
	if err != nil {
		return sr, err
	}
	key := br.String()
	ds.cache.Set(key, key, int64(sr.Size))
	return sr, nil
}

var _ = blobserver.ShutdownStorage((*Storage)(nil))

func (ds *Storage) Close() error {
	ds.cache.Close()
	if ss, ok := ds.bs.(blobserver.ShutdownStorage); ok {
		return ss.Close()
	}
	return nil
}
