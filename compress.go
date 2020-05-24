// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

var archCmd = make(map[string][]string, 2)

func init() {
	archCmd["tar"] = []string{"tar", "cf", "-", "--remove-files", "./"}
	archCmd["zip"] = []string{"zip", "-r", "-m", "-2", "-", "./"}
}
