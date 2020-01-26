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

package percache_test

import (
	"context"
	"io/ioutil"
	"os"
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
	pc, err := percache.New(dn)
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
}
