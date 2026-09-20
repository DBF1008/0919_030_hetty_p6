package sender

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dstotijn/hetty/pkg/reqlog"
)

// RetryConfig controls how the sender retries failed HTTP requests and how
// long a single attempt may take. The zero value is not valid; use
// `DefaultRetryConfig` as a starting point.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts (initial try + retries).
	MaxAttempts int
	// InitialBackoff is the wait duration before the first retry. The backoff
	// doubles after every failed attempt, up to MaxBackoff.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff between attempts.
	MaxBackoff time.Duration
	// PerAttemptTimeout bounds the duration of a single attempt, so one slow
	// request can't block the whole call chain.
	PerAttemptTimeout time.Duration
}

// DefaultRetryConfig returns the default retry configuration for the sender.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    200 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		PerAttemptTimeout: 30 * time.Second,
	}
}

func (rc RetryConfig) withDefaults() RetryConfig {
	if rc.MaxAttempts <= 0 {
		rc.MaxAttempts = 1
	}

	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 200 * time.Millisecond
	}

	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 5 * time.Second
	}

	return rc
}

// sendHTTPRequest sends the HTTP request with retries and a per-attempt
// timeout. Transport level errors and 5xx responses are retried with
// exponential backoff; other responses are returned as-is.
func (svc *Service) sendHTTPRequest(ctx context.Context, httpReq *http.Request) (reqlog.ResponseLog, error) {
	cfg := svc.retryConfig.withDefaults()

	var lastErr error

	backoff := cfg.InitialBackoff

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return reqlog.ResponseLog{}, &SendError{ctx.Err()}
			case <-timer.C:
			}

			backoff *= 2
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}

			// Reset the request body for the next attempt.
			if httpReq.GetBody != nil {
				body, err := httpReq.GetBody()
				if err != nil {
					return reqlog.ResponseLog{}, &SendError{
						fmt.Errorf("failed to reset request body for retry: %w", err),
					}
				}
				httpReq.Body = body
			}
		}

		attemptCtx := ctx
		cancel := context.CancelFunc(func() {})

		if cfg.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, cfg.PerAttemptTimeout)
		}

		resLog, retryable, err := svc.doAttempt(attemptCtx, httpReq)
		cancel()

		if err == nil {
			return resLog, nil
		}

		lastErr = err

		if !retryable {
			break
		}

		// On the final attempt, don't discard a received (5xx) response;
		// it's a valid HTTP response and the caller shouldn't lose it.
		if attempt == cfg.MaxAttempts && resLog.StatusCode > 0 {
			return resLog, nil
		}
	}

	return reqlog.ResponseLog{}, &SendError{
		fmt.Errorf("request failed after %d attempt(s): %w", cfg.MaxAttempts, lastErr),
	}
}

// doAttempt performs a single HTTP request attempt. It reports whether a
// failure is retryable: transport errors and 5xx responses are, everything
// else is not.
func (svc *Service) doAttempt(ctx context.Context, httpReq *http.Request) (reqlog.ResponseLog, bool, error) {
	res, err := svc.httpClient.Do(httpReq.WithContext(ctx))
	if err != nil {
		return reqlog.ResponseLog{}, true, err
	}
	defer res.Body.Close()

	resLog, err := reqlog.ParseHTTPResponse(res)
	if err != nil {
		return reqlog.ResponseLog{}, false, fmt.Errorf("failed to parse http response: %w", err)
	}

	if res.StatusCode >= http.StatusInternalServerError {
		return resLog, true, fmt.Errorf("server responded with status: %v", res.Status)
	}

	return resLog, false, nil
}
