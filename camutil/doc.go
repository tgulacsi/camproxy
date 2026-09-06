// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*
Package camutil copies some unexported utilities from camlistore.org/cmd/cam{get,put}
*/
package camutil

// InsecureTLS sets client's InsecureTLS
var InsecureTLS bool

// SkipIrregular makes camget skip not regular files.
var SkipIrregular bool
