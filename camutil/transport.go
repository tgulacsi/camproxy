// Copyright 2022, 2023 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
	"github.com/rogpeppe/retry"
)

type retryTransport struct {
	retry.Strategy
	tr http.RoundTripper
}

var ErrEmptyResponse = errors.New("empty resonse")

func (tr retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	dur := tr.Strategy.MaxDuration
	if dl, _ := ctx.Deadline(); !dl.IsZero() {
		dur = time.Until(dl) * 9 / 10
	}
	if dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(req.Context(), dur)
		defer cancel()
	}
	if req.Body != nil && req.GetBody == nil {
		var buf bytes.Buffer
		oldBody := req.Body
		req.Body = struct {
			io.Reader
			io.Closer
		}{io.TeeReader(oldBody, &buf), io.NopCloser(nil)}
		req.GetBody = func() (io.ReadCloser, error) {
			return struct {
				io.Reader
				io.Closer
			}{io.MultiReader(bytes.NewReader(buf.Bytes()), oldBody), io.NopCloser(nil)}, nil
		}
	}
	logger := zlog.SFromContext(ctx)
	var resp *http.Response
	var err error
	for iter := tr.Strategy.Start(); ; {
		resp, err = tr.tr.RoundTrip(req)
		if logger != nil && logger.Enabled(ctx, slog.LevelDebug) {
			logger.Debug("RoundTrip", "url", req.URL.String(), "resp", resp, "error", err)
		}
		var sc int
		if resp != nil {
			sc = resp.StatusCode
		}
		if err == nil && resp != nil && sc < 500 {
			return resp, nil
		} else if req.Body != nil && req.GetBody == nil { // We can't repeat this
			if resp == nil && err == nil {
				err = ErrEmptyResponse
			}
			return resp, err
		}
		if req.Body != nil {
			var retryErr error
			if req.Body, retryErr = req.GetBody(); retryErr != nil {
				logger.Error("retry GetBody", "error", retryErr)
				if resp == nil && err == nil {
					err = retryErr
				}
				return resp, err
			}
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		logger.Info("RoundTrip", "url", req.URL.String(), "statusCode", sc, "error", err)
		if !iter.Next(ctx.Done()) {
			break
		}
	}
	if resp == nil && err == nil {
		if err = ctx.Err(); err == nil {
			err = ErrEmptyResponse
		}
	}
	return resp, err
}
