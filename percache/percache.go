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
	"context"
	"io"
	"os"

	badgerdb "github.com/dgraph-io/badger/v2"
	"github.com/tgulacsi/camproxy/blobserver/badger"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
)

const prefix = ","

func New(root string) (*PerCache, error) {
	db, err := badgerdb.Open(badgerdb.DefaultOptions(root))
	if err != nil {
		os.RemoveAll(root)
		os.MkdirAll(root, 0755)
		if db, err = badgerdb.Open(badgerdb.DefaultOptions(root)); err != nil {
			return nil, err
		}
	}
	return &PerCache{db: db, sto: badger.NewManaged(db, "/")}, err
}

type PerCache struct {
	db  *badgerdb.DB
	sto badger.Storage
}

func (pc *PerCache) Close() error {
	firstErr := pc.db.Close()
	if err := pc.sto.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
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
