package sender_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/sender"
)

func fastRetryConfig(maxAttempts int) *sender.RetryConfig {
	return &sender.RetryConfig{
		MaxAttempts:       maxAttempts,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        5 * time.Millisecond,
		PerAttemptTimeout: 5 * time.Second,
	}
}

func storeTestRequest(t *testing.T, repo *fakeRepository, projectID ulid.ULID, targetURL *url.URL) ulid.ULID {
	t.Helper()

	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err := repo.StoreSenderRequest(context.Background(), sender.Request{
		ID:        reqID,
		ProjectID: projectID,
		URL:       targetURL,
		Method:    http.MethodPost,
		Proto:     sender.HTTPProto11,
		Body:      []byte("foobar"),
	})
	if err != nil {
		t.Fatalf("failed to store request: %v", err)
	}

	return reqID
}

// roundTripFunc stubs the HTTP transport, so retry behavior can be tested
// without binding network ports.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func stubResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		StatusCode:    statusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func newTestService(repo *fakeRepository, client *http.Client, retry *sender.RetryConfig, projectID ulid.ULID) *sender.Service {
	svc := sender.NewService(sender.Config{
		Repository: repo,
		HTTPClient: client,
		Retry:      retry,
	})
	svc.SetActiveProjectID(projectID)

	return svc
}

func TestSendRequestRetriesOnServerError(t *testing.T) {
	t.Parallel()

	var attempts int32

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if atomic.AddInt32(&attempts, 1) < 3 {
				return stubResponse(http.StatusBadGateway, ""), nil
			}

			return stubResponse(http.StatusOK, "ok"), nil
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)
	svc := newTestService(repo, client, fastRetryConfig(3), projectID)

	got, err := svc.SendRequest(context.Background(), reqID)
	if err != nil {
		t.Fatalf("unexpected error sending request: %v", err)
	}

	if got.Response == nil || got.Response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK response, got: %+v", got.Response)
	}

	if n := atomic.LoadInt32(&attempts); n != 3 {
		t.Fatalf("expected 3 attempts, got: %d", n)
	}
}

func TestSendRequestReturnsLast5xxResponse(t *testing.T) {
	t.Parallel()

	var attempts int32

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&attempts, 1)
			return stubResponse(http.StatusServiceUnavailable, ""), nil
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)
	svc := newTestService(repo, client, fastRetryConfig(3), projectID)

	got, err := svc.SendRequest(context.Background(), reqID)
	if err != nil {
		t.Fatalf("unexpected error sending request: %v", err)
	}

	if got.Response == nil || got.Response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected last 503 response to be returned, got: %+v", got.Response)
	}

	if n := atomic.LoadInt32(&attempts); n != 3 {
		t.Fatalf("expected 3 attempts, got: %d", n)
	}
}

func TestSendRequestRetriesOnTransportError(t *testing.T) {
	t.Parallel()

	var attempts int32

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if atomic.AddInt32(&attempts, 1) < 3 {
				return nil, errors.New("connection reset")
			}

			return stubResponse(http.StatusOK, "ok"), nil
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)
	svc := newTestService(repo, client, fastRetryConfig(3), projectID)

	got, err := svc.SendRequest(context.Background(), reqID)
	if err != nil {
		t.Fatalf("unexpected error sending request: %v", err)
	}

	if got.Response == nil || got.Response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK response, got: %+v", got.Response)
	}

	if n := atomic.LoadInt32(&attempts); n != 3 {
		t.Fatalf("expected 3 attempts, got: %d", n)
	}
}

func TestSendRequestExhaustsRetries(t *testing.T) {
	t.Parallel()

	var attempts int32

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&attempts, 1)
			return nil, errors.New("connection reset")
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)
	svc := newTestService(repo, client, fastRetryConfig(3), projectID)

	_, err := svc.SendRequest(context.Background(), reqID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var sendErr *sender.SendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("expected `*sender.SendError`, got: %T (%v)", err, err)
	}

	if n := atomic.LoadInt32(&attempts); n != 3 {
		t.Fatalf("expected 3 attempts, got: %d", n)
	}
}

func TestSendRequestPerAttemptTimeout(t *testing.T) {
	t.Parallel()

	var attempts int32

	// The stub transport honors context cancellation, like a real transport.
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&attempts, 1)

			select {
			case <-time.After(500 * time.Millisecond):
				return stubResponse(http.StatusOK, "ok"), nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)

	svc := newTestService(repo, client, &sender.RetryConfig{
		MaxAttempts:       2,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        5 * time.Millisecond,
		PerAttemptTimeout: 50 * time.Millisecond,
	}, projectID)

	start := time.Now()

	_, err := svc.SendRequest(context.Background(), reqID)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("expected per-attempt timeout to bound duration, took: %v", elapsed)
	}

	if n := atomic.LoadInt32(&attempts); n != 2 {
		t.Fatalf("expected 2 attempts, got: %d", n)
	}
}
