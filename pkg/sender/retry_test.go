package sender_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func fakeResponse(req *http.Request, statusCode int) *http.Response {
	return &http.Response{
		Status:        http.StatusText(statusCode),
		StatusCode:    statusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader("")),
		ContentLength: 0,
		Request:       req,
	}
}

// fakeSenderRepo is an in-memory sender.Repository with a configurable
// number of store failures, used to test store compensation.
type fakeSenderRepo struct {
	mu            sync.Mutex
	reqs          map[ulid.ULID]sender.Request
	storeCalls    int
	storeFailures int
}

func newFakeSenderRepo() *fakeSenderRepo {
	return &fakeSenderRepo{reqs: make(map[ulid.ULID]sender.Request)}
}

func (r *fakeSenderRepo) FindSenderRequestByID(_ context.Context, _, id ulid.ULID) (sender.Request, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	req, ok := r.reqs[id]
	if !ok {
		return sender.Request{}, sender.ErrRequestNotFound
	}

	return req, nil
}

func (r *fakeSenderRepo) FindSenderRequests(context.Context, sender.FindRequestsFilter, *scope.Scope) ([]sender.Request, error) {
	return nil, nil
}

func (r *fakeSenderRepo) StoreSenderRequest(_ context.Context, req sender.Request) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.storeCalls++

	if r.storeFailures > 0 {
		r.storeFailures--
		return errors.New("fake repository: transient store error")
	}

	r.reqs[req.ID] = req

	return nil
}

func (r *fakeSenderRepo) DeleteSenderRequests(context.Context, ulid.ULID) error {
	return nil
}

func (r *fakeSenderRepo) storedReq(t *testing.T, id ulid.ULID) sender.Request {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	req, ok := r.reqs[id]
	if !ok {
		t.Fatalf("expected request %v to be stored", id)
	}

	return req
}

// fakeReqLogRepo is an in-memory reqlog.Repository that hands out shared
// header and body references, used to verify clone deep-copying.
type fakeReqLogRepo struct {
	reqLog reqlog.RequestLog
}

func (r *fakeReqLogRepo) FindRequestLogs(context.Context, reqlog.FindRequestsFilter, *scope.Scope) ([]reqlog.RequestLog, error) {
	return nil, nil
}

func (r *fakeReqLogRepo) FindRequestLogByID(_ context.Context, _, id ulid.ULID) (reqlog.RequestLog, error) {
	if id != r.reqLog.ID {
		return reqlog.RequestLog{}, reqlog.ErrRequestNotFound
	}

	return r.reqLog, nil
}

func (r *fakeReqLogRepo) StoreRequestLog(context.Context, reqlog.RequestLog) error {
	return nil
}

func (r *fakeReqLogRepo) StoreResponseLog(context.Context, ulid.ULID, ulid.ULID, reqlog.ResponseLog) error {
	return nil
}

func (r *fakeReqLogRepo) ClearRequestLogs(context.Context, ulid.ULID) error {
	return nil
}

func setupSendTest(t *testing.T, repo *fakeSenderRepo, transport http.RoundTripper, retry sender.RetryConfig) (sender.Request, *sender.Service) {
	t.Helper()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)

	req := sender.Request{
		ID:        reqID,
		ProjectID: projectID,
		URL:       exampleURL,
		Method:    http.MethodPost,
		Proto:     sender.HTTPProto11,
		Body:      []byte("foobar"),
	}

	if err := repo.StoreSenderRequest(context.Background(), req); err != nil {
		t.Fatalf("failed to seed request: %v", err)
	}

	svc := sender.NewService(sender.Config{
		Repository: repo,
		HTTPClient: &http.Client{Transport: transport},
		Retry:      retry,
	})
	svc.SetActiveProjectID(projectID)

	return req, svc
}

func TestSendRequestRetriesOnTransportError(t *testing.T) {
	t.Parallel()

	var attempts int

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("connection reset by peer")
		}

		return fakeResponse(req, http.StatusOK), nil
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	got, err := svc.SendRequest(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got: %d", attempts)
	}

	if got.Response == nil || got.Response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 response, got: %+v", got.Response)
	}

	stored := repo.storedReq(t, req.ID)
	if stored.Response == nil {
		t.Fatal("expected response to be persisted")
	}
}

func TestSendRequestRetriesOnRetryableStatus(t *testing.T) {
	t.Parallel()

	var attempts int

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if attempts < 3 {
			return fakeResponse(req, http.StatusServiceUnavailable), nil
		}

		return fakeResponse(req, http.StatusOK), nil
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	got, err := svc.SendRequest(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got: %d", attempts)
	}

	if got.Response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 response, got: %d", got.Response.StatusCode)
	}
}

func TestSendRequestRetryExhaustedReturnsLastResponse(t *testing.T) {
	t.Parallel()

	var attempts int

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		return fakeResponse(req, http.StatusServiceUnavailable), nil
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	got, err := svc.SendRequest(context.Background(), req.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got: %d", attempts)
	}

	if got.Response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected last 503 response, got: %d", got.Response.StatusCode)
	}
}

func TestSendRequestNoRetryByDefault(t *testing.T) {
	t.Parallel()

	var attempts int

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		return nil, errors.New("connection refused")
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{})

	_, err := svc.SendRequest(context.Background(), req.ID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var sendErr *sender.SendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("expected sender.SendError, got: %v", err)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt (retries disabled), got: %d", attempts)
	}
}

func TestSendRequestAttemptTimeout(t *testing.T) {
	t.Parallel()

	var attempts int

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		<-req.Context().Done()

		return nil, req.Context().Err()
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{
		MaxAttempts:    2,
		AttemptTimeout: 50 * time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	start := time.Now()

	_, err := svc.SendRequest(context.Background(), req.ID)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("expected attempts to time out quickly, took: %v", elapsed)
	}

	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got: %d", attempts)
	}
}

func TestSendRequestResendsBodyOnRetry(t *testing.T) {
	t.Parallel()

	var bodies [][]byte

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}

		bodies = append(bodies, body)

		if len(bodies) < 2 {
			return nil, errors.New("connection reset")
		}

		return fakeResponse(req, http.StatusOK), nil
	})

	repo := newFakeSenderRepo()
	req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	if _, err := svc.SendRequest(context.Background(), req.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(bodies) != 2 {
		t.Fatalf("expected 2 attempts, got: %d", len(bodies))
	}

	for i, body := range bodies {
		if string(body) != "foobar" {
			t.Fatalf("attempt %d sent wrong body: %q", i+1, body)
		}
	}
}

func TestSendRequestStoreCompensation(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return fakeResponse(req, http.StatusOK), nil
	})

	t.Run("transient store failures are retried", func(t *testing.T) {
		t.Parallel()

		repo := newFakeSenderRepo()
		req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{})

		repo.mu.Lock()
		repo.storeFailures = 2
		repo.mu.Unlock()

		got, err := svc.SendRequest(context.Background(), req.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got.Response == nil {
			t.Fatal("expected response")
		}

		stored := repo.storedReq(t, req.ID)
		if stored.Response == nil {
			t.Fatal("expected response to be persisted after store retries")
		}
	})

	t.Run("persistent store failure returns response with error", func(t *testing.T) {
		t.Parallel()

		repo := newFakeSenderRepo()
		req, svc := setupSendTest(t, repo, transport, sender.RetryConfig{})

		repo.mu.Lock()
		repo.storeFailures = 100
		repo.mu.Unlock()

		got, err := svc.SendRequest(context.Background(), req.ID)
		if !errors.Is(err, sender.ErrResponseNotStored) {
			t.Fatalf("expected sender.ErrResponseNotStored, got: %v", err)
		}

		// Compensation: the response is not lost, it's returned to the caller.
		if got.Response == nil || got.Response.StatusCode != http.StatusOK {
			t.Fatalf("expected response to be returned despite store failure, got: %+v", got.Response)
		}
	})
}

func TestCloneFromRequestLogDeepCopy(t *testing.T) {
	t.Parallel()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqLogID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)

	reqLogRepo := &fakeReqLogRepo{
		reqLog: reqlog.RequestLog{
			ID:        reqLogID,
			ProjectID: projectID,
			URL:       exampleURL,
			Method:    http.MethodPost,
			Proto:     "HTTP/1.1",
			Header: http.Header{
				"X-Foo": []string{"bar"},
			},
			Body: []byte("foobar"),
		},
	}

	svc := sender.NewService(sender.Config{
		Repository: newFakeSenderRepo(),
		ReqLogService: reqlog.NewService(reqlog.Config{
			ActiveProjectID: projectID,
			Repository:      reqLogRepo,
		}),
	})
	svc.SetActiveProjectID(projectID)

	clone, err := svc.CloneFromRequestLog(context.Background(), reqLogID)
	if err != nil {
		t.Fatalf("unexpected error cloning from request log: %v", err)
	}

	// Mutating the clone must not affect the source request log.
	clone.Header.Set("X-Foo", "mutated")
	clone.Body[0] = 'X'

	if got := reqLogRepo.reqLog.Header.Get("X-Foo"); got != "bar" {
		t.Fatalf("source request log header was mutated: %q", got)
	}

	if got := string(reqLogRepo.reqLog.Body); got != "foobar" {
		t.Fatalf("source request log body was mutated: %q", got)
	}
}
