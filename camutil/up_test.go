// Copyright 2013, 2023 Tamás Gulácsi.
//
// SPDX-License-Identifier: EUPL-1.2

package camutil

import (
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

	u := NewUploader("file://"+tempDir, WithCapCtime(true), WithSkipHaveCache(true))
	defer u.Close()
	ctx := t.Context()
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
