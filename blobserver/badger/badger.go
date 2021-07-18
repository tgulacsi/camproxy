// Copyright 2020, 2021 The Perkeep Authors
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

/*
Package badger implements the blobserver interface by storing each blob
in a dgraph-io/badger database.
*/
package badger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"

	badgerdb "github.com/dgraph-io/badger/v3"
)

var _ = blobserver.Storage(Storage{})

type Storage struct {
	db     *badgerdb.DB
	prefix string
}

// NewManaged uses the given badgerdb.DB.
func NewManaged(db *badgerdb.DB, prefix string) Storage {
	return Storage{db: db, prefix: prefix}
}
func New(root, prefix string) (Storage, error) {
	db, err := badgerdb.Open(badgerdb.DefaultOptions(root).WithLogger(nilLogger{}))
	if err != nil {
		os.RemoveAll(root)
		os.MkdirAll(root, 0755)
		if db, err = badgerdb.Open(badgerdb.DefaultOptions(root)); err != nil {
			return Storage{}, err
		}
	}
	return NewManaged(db, prefix), nil
}

func (sto Storage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	key := sto.blobRefBytes(br)
	var value []byte
	err := sto.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(value)
		return err
	})
	return struct {
		io.Reader
		io.Closer
	}{bytes.NewReader(value), ioutil.NopCloser(nil)}, uint32(len(value)), err
}

func (sto Storage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	return sto.db.Update(func(txn *badgerdb.Txn) error {
		var firstErr error
		for _, br := range blobs {
			if err := txn.Delete(sto.blobRefBytes(br)); err != nil && firstErr == nil {
				firstErr = err
			}
			select {
			case <-ctx.Done():
				txn.Discard()
				return ctx.Err()
			default:
			}
		}
		return firstErr
	})
}

func (sto Storage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	return sto.db.View(func(txn *badgerdb.Txn) error {
		for _, br := range blobs {
			item, err := txn.Get(sto.blobRefBytes(br))
			if err != nil {
				continue
			}
			if err = fn(blob.SizedRef{Ref: br, Size: uint32(item.ValueSize())}); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}

func (sto Storage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	opts := badgerdb.DefaultIteratorOptions
	opts.PrefetchValues, opts.PrefetchSize, opts.Prefix = true, 16, []byte(sto.prefix)
	return sto.db.View(func(txn *badgerdb.Txn) error {
		defer close(dest)
		it := txn.NewIterator(opts)
		defer it.Close()
		var n int
		it.Rewind()
		if after != "" && it.Valid() {
			if br, ok := blob.Parse(after); ok {
				it.Seek(sto.blobRefBytes(br))
				if it.Valid() {
					it.Next()
				}
			}
		}
		if !it.Valid() {
			return nil
		}
		for ; it.Valid(); it.Next() {
			item := it.Item()
			var ref blob.Ref
			err := ref.UnmarshalBinary(bytes.TrimPrefix(item.Key(), []byte(sto.prefix)))
			if err != nil {
				continue
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- blob.SizedRef{Ref: ref, Size: uint32(item.ValueSize())}:
				n++
				if limit != 0 && n >= limit {
					return nil
				}
			}
		}
		return nil
	})
}

var _ = blobserver.Generationer((*Storage)(nil))

func (sto Storage) StorageGeneration() (initTime time.Time, random string, err error) {
	err = sto.db.Update(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(sto.prefix + "GENERATION"))
		if err != nil {
			return err
		}
		var gen generation
		return item.Value(func(value []byte) error {
			if err = json.Unmarshal(value, &gen); err != nil {
				return err
			}
			initTime, random = gen.InitTime, gen.Random
			return nil
		})
	})
	return
}

type generation struct {
	InitTime time.Time
	Random   string
}

func (sto Storage) ResetStorageGeneration() error {
	p := make([]byte, 16)
	_, _ = rand.Read(p)
	b, err := json.Marshal(generation{InitTime: time.Now(), Random: string(p)})
	if err != nil {
		return err
	}
	return sto.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Set([]byte(sto.prefix+"GENERATION"), b)
	})
}

func (sto Storage) ReceiveBlob(ctx context.Context, br blob.Ref, source io.Reader) (blob.SizedRef, error) {
	b, err := ioutil.ReadAll(source)
	if err != nil {
		return blob.SizedRef{Ref: br}, err
	}
	return blob.SizedRef{Ref: br, Size: uint32(len(b))},
		sto.db.Update(func(txn *badgerdb.Txn) error {
			key := sto.blobRefBytes(br)
			return txn.Set(key, b)
		})
}
func BlobRefBytes(prefix string, br blob.Ref) []byte {
	b, _ := br.MarshalBinary()
	return append(append(make([]byte, 0, len(prefix)+len(b)), prefix...), b...)
}

func (sto Storage) blobRefBytes(br blob.Ref) []byte {
	return BlobRefBytes(sto.prefix, br)
}

var _ = blobserver.ShutdownStorage((*Storage)(nil))

func (sto Storage) Close() error {
	return sto.db.Close()
}

type nilLogger struct{}

func (nilLogger) Errorf(string, ...interface{})   {}
func (nilLogger) Warningf(string, ...interface{}) {}
func (nilLogger) Infof(string, ...interface{})    {}
func (nilLogger) Debugf(string, ...interface{})   {}
