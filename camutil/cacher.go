// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/tgulacsi/camproxy/percache"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/cacher"
)

func NewBadgerCache(fetcher blob.Fetcher) (*BadgerCache, error) {
	var cacheDir string
	if dn, err := os.UserCacheDir(); err == nil {
		cacheDir = filepath.Join(dn, "blobs")
		if fi, err := os.Stat(cacheDir); err != nil || !fi.Mode().IsDir() {
			if err := os.Mkdir(cacheDir, 0700); err != nil {
				log.Printf("Warning: failed to make %s: %v; using tempdir instead", cacheDir, err)
				cacheDir = ""
			}
		}
	}
	if cacheDir == "" {
		var err error
		if cacheDir, err = ioutil.TempDir("", "camlicache"); err != nil {
			return nil, err
		}
	}

	diskcache, err := percache.New(cacheDir, 10000, 1<<30)
	if err != nil {
		return nil, err
	}
	dc := &BadgerCache{
		CachingFetcher: cacher.NewCachingFetcher(diskcache, fetcher),
		Root:           cacheDir,
	}
	return dc, nil
}

type BadgerCache struct {
	*cacher.CachingFetcher

	// Root is the temp directory being used to store files.
	// It is available mostly for debug printing.
	Root string
}
