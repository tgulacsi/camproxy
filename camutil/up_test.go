// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestNewPermanode(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "camli-")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	oLog := Log
	logM := make(map[string]interface{})
	Log = func(keyvals ...interface{}) error {
		t.Helper()
		for k := range logM {
			delete(logM, k)
		}
		for i := 0; i < len(keyvals); i += 2 {
			logM[keyvals[i].(string)] = keyvals[i+1]
		}
		b, err := json.Marshal(logM)
		t.Log(string(b))
		return err
	}
	defer func() { Log = oLog }()

	u := NewUploader("file://"+tempDir, true, true)
	defer u.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	contentKey, err := u.FromReader(ctx, "test.txt", strings.NewReader("nothing"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("contentKey=%v", contentKey)
	permaKey, err := u.NewPermanode(ctx, map[string]string{"an attr": "ibute"})
	if err != nil {
		t.Error(err)
	}
	t.Logf("permaKey=%v", permaKey)
}
