package camutil

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestNewPermanode(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "camli-")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	u := NewUploader("file://"+tempDir, true, true)
	defer u.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	contentKey, err := u.FromReader(ctx, "test.txt", strings.NewReader("nothing"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("contentKey=%v", contentKey)
	permaKey, err := u.NewPermanode(ctx, map[string]string{"an attr": "ibute"})
	if err != nil {
		t.Error(err)
	}
	t.Logf("permaKey=%v", permaKey)
}
