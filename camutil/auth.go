// Copyright 2013, 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net/http"
	"strings"

	auth "github.com/abbot/go-http-auth"
)

// SetupBasicAuthChecker sets up a HTTP Basic authentication checker with the
// given camliAuth userpass:username:password[:+localhost,vivify=true]
// (see CAMLI_AUTH) string
func SetupBasicAuthChecker(handler http.HandlerFunc, camliAuth string) http.HandlerFunc {
	if camliAuth == "" {
		return handler
	}
	parts := strings.Split(camliAuth, ":")
	if len(parts) < 3 || parts[0] != "userpass" {
		Log("msg", "unrecognizable camliAuth "+camliAuth)
		return handler
	}
	username := parts[1]
	hsh := sha1.New()
	if _, err := io.WriteString(hsh, parts[2]); err != nil {
		Log("msg", "error hashing user:passw", "error", err)
		return nil
	}
	passwd := "{SHA}" + base64.StdEncoding.EncodeToString(hsh.Sum(nil))
	authenticator := auth.NewBasicAuthenticator("camproxy",
		func(user, realm string) string {
			if user == username {
				return passwd
			}
			return ""
		})
	return auth.JustCheck(authenticator, handler)
}
