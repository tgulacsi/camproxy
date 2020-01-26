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
Package trace implements the blobserver interface which traces each and every call
of the interface by calling the specified functions - then passing the call to the underlying storage.
*/
package trace

import (
	"context"
	"io"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
)

var _ = blobserver.Storage(Storage{})

type Storage struct {
	Storage                                           blobserver.Storage
	OnFetch, OnRemove, OnStat, OnEnumerate, OnReceive func([]blob.SizedRef, error)
}

func (sto Storage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	rc, size, err := sto.Storage.Fetch(ctx, br)
	if sto.OnFetch != nil {
		sto.OnFetch([]blob.SizedRef{{Ref: br, Size: size}}, err)
	}
	return rc, size, err
}

func (sto Storage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	err := sto.Storage.RemoveBlobs(ctx, blobs)
	if sto.OnRemove != nil {
		sized := make([]blob.SizedRef, len(blobs))
		for i, br := range blobs {
			sized[i] = blob.SizedRef{Ref: br}
		}
		sto.OnRemove(sized, err)
	}
	return err
}

func (sto Storage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	if sto.OnStat == nil {
		return sto.Storage.StatBlobs(ctx, blobs, fn)
	}
	sized := make([]blob.SizedRef, 0, len(blobs))
	err := sto.Storage.StatBlobs(ctx, blobs, func(sr blob.SizedRef) error {
		if err := fn(sr); err != nil {
			return err
		}
		sized = append(sized, sr)
		return nil
	})
	sto.OnStat(sized, err)
	return err
}

func (sto Storage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	if sto.OnEnumerate == nil {
		return sto.Storage.EnumerateBlobs(ctx, dest, after, limit)
	}
	ch := make(chan blob.SizedRef)
	var sized []blob.SizedRef
	go func() {
		defer close(ch)
		for sr := range ch {
			dest <- sr
			sized = append(sized, sr)
		}
	}()
	err := sto.Storage.EnumerateBlobs(ctx, ch, after, limit)
	sto.OnEnumerate(sized, err)
	return err
}

func (sto Storage) ReceiveBlob(ctx context.Context, br blob.Ref, source io.Reader) (blob.SizedRef, error) {
	sr, err := sto.Storage.ReceiveBlob(ctx, br, source)
	if sto.OnReceive != nil {
		sto.OnReceive([]blob.SizedRef{sr}, err)
	}
	return sr, err
}

var _ = blobserver.ShutdownStorage((*Storage)(nil))

func (sto Storage) Close() error {
	if cl, ok := sto.Storage.(blobserver.ShutdownStorage); ok {
		return cl.Close()
	}
	return nil
}

var _ = blobserver.Generationer((*Storage)(nil))

func (sto Storage) StorageGeneration() (initTime time.Time, random string, err error) {
	if sg, ok := sto.Storage.(blobserver.Generationer); ok {
		return sg.StorageGeneration()
	}
	err = blobserver.ErrNotImplemented
	return
}

func (sto Storage) ResetStorageGeneration() error {
	if sg, ok := sto.Storage.(blobserver.Generationer); ok {
		return sg.ResetStorageGeneration()
	}
	return blobserver.ErrNotImplemented
}
