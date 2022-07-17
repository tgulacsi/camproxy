//go:build !windows
// +build !windows

// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

// copied from camlistore.org/pkg/blobserver/localdisk/receive.go

import (
	"os"
	"runtime"
	"syscall"
)

// LinkOrCopy links src to dst if possible; copies if not
func LinkOrCopy(src, dst string) error {
	err := os.Link(src, dst)
	if le, ok := err.(*os.LinkError); ok && le.Op == "link" && le.Err == syscall.Errno(0x26) && runtime.GOOS == "linux" {
		// Whatever 0x26 is, it's returned by Linux when the underlying
		// filesystem (e.g. exfat) doesn't support link.
		return CopyFile(src, dst)
	}
	return err
}
