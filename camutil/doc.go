// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

/*
Package camutil copies some unexported utilities from camlistore.org/cmd/cam{get,put}
*/
package camutil

// Verbose shall be true for verbose HTTP logging
var Verbose = false

// InsecureTLS sets client's InsecureTLS
var InsecureTLS bool

// SkipIrregular makes camget skip not regular files.
var SkipIrregular bool
