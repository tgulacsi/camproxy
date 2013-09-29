package camutil

import (
	"testing"
)

func TestBase64ToHex(t *testing.T) {
	for i, fromto := range [][2]string{{"sHa1-9sfOFOkcUBM2igo8PCS9aWd42CM=", "sha1-f6c7ce14e91c5013368a0a3c3c24bd696778d823"}} {
		br, err := Base64ToRef(fromto[0])
		if err != nil {
			t.Errorf("%d. %q: %s", i, fromto[0], err)
			continue
		}
		if br.String() != fromto[1] {
			t.Errorf("%d. wanted %q, got %q", i, fromto[0], br.String())
		}
	}
}
