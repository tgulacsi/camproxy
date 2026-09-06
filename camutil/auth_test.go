// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: EUPL-1.2

package camutil

import "testing"

func TestSetupBasicAuthChecker(t *testing.T) {
	for i, elt := range []struct {
		auth  string
		nilOK bool
	}{
		{"", true},
		{"userpass:a:b", false},
	} {
		if SetupBasicAuthChecker(nil, elt.auth) == nil && !elt.nilOK {
			t.Errorf("%d. nil handler from %q", i, elt.auth)
		}
	}
}
