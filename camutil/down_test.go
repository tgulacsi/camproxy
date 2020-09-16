// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"testing"
)

var testb64hex = [][2]string{
	{"sha1-9sfOFOkcUBM2igo8PCS9aWd42CM=", "sha1-f6c7ce14e91c5013368a0a3c3c24bd696778d823"},
	// openssl rand | openssl sha224 | xxd -r -p | base64 | tr '+/' '-_'
	{"sha224-0UoCjCo6K8lHYQK7KII0xBWisB-CjqYqxbPkLw==", "sha224-d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"},
}

func TestBase64ToHex(t *testing.T) {
	t.Parallel()
	for i, fromto := range testb64hex {
		br, err := Base64ToRef(fromto[0])
		if err != nil {
			t.Errorf("%d. %q: %s", i+1, fromto[0], err)
			continue
		}
		if br.String() != fromto[1] {
			t.Errorf("%d. wanted %q, got %q", i+1, fromto[1], br.String())
		}

		if got := RefToBase64(br); got != fromto[0] {
			t.Errorf("%d. wanted %q, got %q", i+1, fromto[0], got)
		}
	}
}
