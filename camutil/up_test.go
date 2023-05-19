// Copyright 2013, 2023 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/UNO-SOFT/zlog/v2"
)

func TestNewPermanode(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "camli-")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger = zlog.NewT(t).SLog()

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
