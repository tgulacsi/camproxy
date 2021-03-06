// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package percache_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tgulacsi/camproxy/percache"
)

func TestCache(t *testing.T) {
	dn, err := ioutil.TempDir("", "percache-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dn)
	pc, err := percache.New(dn, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for k, v := range map[string]string{
		"a": "árvíztűrő tükörfúrógép",
		"b": "nil",
	} {
		if err := pc.Put(ctx, k, strings.NewReader(v)); err != nil {
			t.Errorf("Put %q: %+v", k, err)
			continue
		}
		rc, err := pc.Get(ctx, k)
		if err != nil {
			t.Errorf("Get %q: %+v", k, err)
			continue
		}
		b, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Errorf("Read %q: %+v", k, err)
			continue
		}
		if string(b) != v {
			t.Errorf("%q: got %q, wanted %q", k, string(b), v)
		}
	}

	var a [4]byte
	var b []byte
	for i := 0; i < 1000; i++ {
		_, _ = rand.Read(a[:])
		b = append(b, a[:]...)
		if err := pc.Put(ctx, strconv.Itoa(i), bytes.NewReader(b)); err != nil {
			t.Errorf("Put: %d: %+v", i, err)
		}
	}
}
