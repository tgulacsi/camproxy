// Copyright 2022, 2023 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package camutil

import (
	"context"
	"net/http"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
	"github.com/rogpeppe/retry"
)

type retryTransport struct {
	retry.Strategy
	tr http.RoundTripper
}

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
	logger := zlog.SFromContext(ctx)
	var resp *http.Response
	var err error
	for iter := tr.Strategy.Start(); iter.Next(ctx.Done()); {
		resp, err = tr.tr.RoundTrip(req)
		var sc int
		if resp != nil {
			sc = resp.StatusCode
		}
		if err == nil && resp != nil && sc < 500 || req.Body != nil && req.GetBody == nil {
			return resp, err
		}
		if req.Body != nil {
			var retryErr error
			if req.Body, retryErr = req.GetBody(); retryErr != nil {
				logger.Error("retry GetBody", "error", retryErr)
				return resp, err
			}
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		logger.Info("RoundTrip", "url", req.URL.String(), "statusCode", sc, "error", err)
	}
	return resp, err
}
