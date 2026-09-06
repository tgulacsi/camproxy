// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

package camutil

// copied from camlistore.org/pkg/blobserver/localdisk/receive.go

// LinkOrCopy copies src to dst (on Windows no link is possible)
func LinkOrCopy(src, dst string) error {
	return CopyFile(src, dst)
}
