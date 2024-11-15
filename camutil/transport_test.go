// Copyright 2022, 2023 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
	"github.com/rogpeppe/retry"
)

func TestRetryTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	logger := zlog.NewT(t).SLog()
	zlog.NewSContext(ctx, logger)
	tr := retryTransport{
		Strategy: retry.Strategy{
			Delay: 100 * time.Millisecond, MaxDelay: 5 * time.Second,
			Factor: 1.5, MaxCount: 10,
			MaxDuration: 10 * time.Second,
		},
		tr: testTransport{
			Err: randomError(0.85),
			Response: &http.Response{Status: "200 OK", StatusCode: 200,
				Body: struct {
					io.Reader
					io.Closer
				}{strings.NewReader("test ok"), io.NopCloser(nil)}},
		}}
	cl := http.Client{Transport: tr}
	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cl.Do(req)
	t.Logf("resp=%#v err=%#v", resp, err)
}

type testTransport struct {
	Err      func() error
	Response *http.Response
}

func (tr testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := tr.Err(); err != nil {
		return nil, err
	}
	return tr.Response, nil
}

func randomError(okProb float64) func() error {
	return func() error {
		f := rand.Float64()
		if f < -okProb || okProb < f {
			return nil
		}
		return fmt.Errorf("%s: prob=%f", io.ErrUnexpectedEOF, f)
	}
}
