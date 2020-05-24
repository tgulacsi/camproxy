// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

// copied from camlistore.org/pkg/blobserver/localdisk/receive.go

// LinkOrCopy copies src to dst (on Windows no link is possible)
func LinkOrCopy(src, dst string) error {
	return CopyFile(src, dst)
}
