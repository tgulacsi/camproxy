// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: EUPL-1.2

package camutil

// copied from camlistore.org/pkg/blobserver/localdisk/receive.go

// LinkOrCopy copies src to dst (on Windows no link is possible)
func LinkOrCopy(src, dst string) error {
	return CopyFile(src, dst)
}
