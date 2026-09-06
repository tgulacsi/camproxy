// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: EUPL-1.2

/*
Package camutil copies some unexported utilities from camlistore.org/cmd/cam{get,put}
*/
package camutil

// InsecureTLS sets client's InsecureTLS
var InsecureTLS bool

// SkipIrregular makes camget skip not regular files.
var SkipIrregular bool
