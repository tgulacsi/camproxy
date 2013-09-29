package main

import (
	"errors"
	"io"
	"os/exec"
)

var archCmd = make(map[string][]string, 2)

func init() {
	archCmd["tar"] = []string{"tar", "cf", "-", "--remove-files", "./"}
	archCmd["zip"] = []string{"zip", "-r", "-m", "-2", "-", "./"}
}

func tarDir(w io.Writer, dn string) error {
	return externalArchDir(w, dn, "tar")
}
func zipDir(w io.Writer, dn string) error {
	return externalArchDir(w, dn, "zip")
}

func externalArchDir(w io.Writer, dn, method string) error {
	cmd, ok := archCmd[method]
	if !ok {
		return errors.New("unknown method " + method)
	}
	c := exec.Command(cmd[0], cmd[1:]...)
	c.Dir = dn
	c.Stdout = w
	return c.Run()
}
