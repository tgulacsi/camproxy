// Copyright 2020, 2021 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package percache_test

import (
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
	pc, err := percache.New(dn, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	pc.Logger = testLogger{TB: t}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	putGet := func(k, v string) {
		if err := pc.Put(ctx, k, strings.NewReader(v)); err != nil {
			t.Errorf("Put %q: %+v", k, err)
			return
		}
		rc, err := pc.Get(ctx, k)
		if err != nil {
			t.Errorf("Get %q: %+v", k, err)
			return
		}
		b, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Errorf("Read %q: %+v", k, err)
			return
		}
		if string(b) != v {
			t.Errorf("%q: got %q, wanted %q", k, string(b), v)
		}
	}

	for k, v := range map[string]string{
		"a": "árvíztűrő tükörfúrógép",
		"b": "nil",
	} {
		putGet(k, v)
	}

	var a [4]byte
	var b []byte
	for i := 0; i < 1000; i++ {
		_, _ = rand.Read(a[:])
		b = append(b, a[:]...)
		putGet(strconv.Itoa(i), string(b))
	}
}

type testLogger struct{ testing.TB }

func (tl testLogger) Log(keyvals ...interface{}) error {
	tl.TB.Helper()
	tl.TB.Log(keyvals...)
	return nil
}
