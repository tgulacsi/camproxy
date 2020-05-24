// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

// copied from camlistore.org/pkg/blobserver/localdisk/receive.go

import (
	"io"
	"os"
)

// CopyFile is used by Windows (receive_windows.go) and when a posix filesystem doesn't
// support a link operation (e.g. Linux with an exfat external USB disk).
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
