package reqlog_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

// fakeReqLogRepo is an in-memory reqlog.Repository.
type fakeReqLogRepo struct {
	mu   sync.Mutex
	logs map[ulid.ULID]reqlog.RequestLog
}

func (r *fakeReqLogRepo) FindRequestLogs(context.Context, reqlog.FindRequestsFilter, *scope.Scope) ([]reqlog.RequestLog, error) {
	return nil, nil
}

func (r *fakeReqLogRepo) FindRequestLogByID(_ context.Context, _, id ulid.ULID) (reqlog.RequestLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reqLog, ok := r.logs[id]
	if !ok {
		return reqlog.RequestLog{}, reqlog.ErrRequestNotFound
	}

	return reqLog, nil
}

func (r *fakeReqLogRepo) StoreRequestLog(_ context.Context, reqLog reqlog.RequestLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logs[reqLog.ID] = reqLog

	return nil
}

func (r *fakeReqLogRepo) StoreResponseLog(context.Context, ulid.ULID, ulid.ULID, reqlog.ResponseLog) error {
	return nil
}

func (r *fakeReqLogRepo) ClearRequestLogs(context.Context, ulid.ULID) error {
	return nil
}

func (r *fakeReqLogRepo) storedBody(t *testing.T, id ulid.ULID) []byte {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	reqLog, ok := r.logs[id]
	if !ok {
		t.Fatalf("expected request log %v to be stored", id)
	}

	return reqLog.Body
}

func setupReqLogSvc(t *testing.T) (*reqlog.Service, *fakeReqLogRepo) {
	t.Helper()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	repo := &fakeReqLogRepo{logs: make(map[ulid.ULID]reqlog.RequestLog)}

	svc := reqlog.NewService(reqlog.Config{
		ActiveProjectID: projectID,
		Repository:      repo,
		Scope:           &scope.Scope{},
	})

	return svc, repo
}

func TestRequestModifierSetsGetBody(t *testing.T) {
	t.Parallel()

	svc, repo := setupReqLogSvc(t)

	next := func(req *http.Request) {}
	reqModFn := svc.RequestModifier(next)

	req := httptest.NewRequest("POST", "https://example.com/", strings.NewReader("foobar"))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	if req.GetBody == nil {
		t.Fatal("expected req.GetBody to be set after request modifier")
	}

	rc, err := req.GetBody()
	if err != nil {
		t.Fatalf("unexpected GetBody error: %v", err)
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("unexpected error re-reading body: %v", err)
	}

	if string(body) != "foobar" {
		t.Fatalf("expected re-read body %q, got: %q", "foobar", body)
	}

	if got := string(repo.storedBody(t, reqID)); got != "foobar" {
		t.Fatalf("expected stored body %q, got: %q", "foobar", got)
	}
}

func TestRequestModifierRecoversConsumedBody(t *testing.T) {
	t.Parallel()

	svc, repo := setupReqLogSvc(t)

	// Simulate an upstream modifier that consumes the request body without
	// resetting it.
	next := func(req *http.Request) {
		_, _ = io.ReadAll(req.Body)
	}
	reqModFn := svc.RequestModifier(next)

	req := httptest.NewRequest("POST", "https://example.com/", strings.NewReader("foobar"))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("foobar")), nil
	}
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	if got := string(repo.storedBody(t, reqID)); got != "foobar" {
		t.Fatalf("expected stored body recovered via GetBody %q, got: %q", "foobar", got)
	}

	// The outbound request body must be intact for the next handler.
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("unexpected error reading outbound body: %v", err)
	}

	if string(body) != "foobar" {
		t.Fatalf("expected outbound body %q, got: %q", "foobar", body)
	}
}
