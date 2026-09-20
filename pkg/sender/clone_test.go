package sender_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

// fakeReqlogRepository returns the same `RequestLog` value (with shared
// Header and Body references) on every call, to simulate a request log
// service whose returned logs share memory with the caller.
type fakeReqlogRepository struct {
	reqLog *reqlog.RequestLog
}

func (r *fakeReqlogRepository) FindRequestLogs(_ context.Context, _ reqlog.FindRequestsFilter, _ *scope.Scope) ([]reqlog.RequestLog, error) {
	return []reqlog.RequestLog{*r.reqLog}, nil
}

func (r *fakeReqlogRepository) FindRequestLogByID(_ context.Context, _, _ ulid.ULID) (reqlog.RequestLog, error) {
	return *r.reqLog, nil
}

func (r *fakeReqlogRepository) StoreRequestLog(_ context.Context, _ reqlog.RequestLog) error {
	return nil
}

func (r *fakeReqlogRepository) StoreResponseLog(_ context.Context, _, _ ulid.ULID, _ reqlog.ResponseLog) error {
	return nil
}

func (r *fakeReqlogRepository) ClearRequestLogs(_ context.Context, _ ulid.ULID) error {
	return nil
}

func TestCloneFromRequestLogDeepCopy(t *testing.T) {
	t.Parallel()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqLogID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)

	sharedReqLog := &reqlog.RequestLog{
		ID:        reqLogID,
		ProjectID: projectID,
		URL:       exampleURL,
		Method:    http.MethodPost,
		Proto:     "HTTP/1.1",
		Header: http.Header{
			"X-Foo": []string{"bar"},
		},
		Body: []byte("foobar"),
	}

	reqLogSvc := reqlog.NewService(reqlog.Config{
		ActiveProjectID: projectID,
		Repository:      &fakeReqlogRepository{reqLog: sharedReqLog},
	})

	repo := newFakeRepository()

	svc := sender.NewService(sender.Config{
		Repository:    repo,
		ReqLogService: reqLogSvc,
	})
	svc.SetActiveProjectID(projectID)

	ctx := context.Background()

	first, err := svc.CloneFromRequestLog(ctx, reqLogID)
	if err != nil {
		t.Fatalf("unexpected error cloning from request log: %v", err)
	}

	// Mutate the cloned request's header and body.
	first.Header.Set("X-Foo", "mutated")
	first.Body[0] = 'X'

	second, err := svc.CloneFromRequestLog(ctx, reqLogID)
	if err != nil {
		t.Fatalf("unexpected error cloning from request log: %v", err)
	}

	// The second clone must not see mutations of the first clone: both the
	// header and the body must be deep copies of the request log's data.
	if got := second.Header.Get("X-Foo"); got != "bar" {
		t.Fatalf("expected second clone header %q, got: %q", "bar", got)
	}

	if got := string(second.Body); got != "foobar" {
		t.Fatalf("expected second clone body %q, got: %q", "foobar", got)
	}

	// Clones must not share header maps or body slices with each other.
	second.Header.Set("X-Foo", "second")
	second.Body[0] = 'Y'

	if got := first.Header.Get("X-Foo"); got != "mutated" {
		t.Fatalf("expected clones to not share headers, first clone header changed to: %q", got)
	}

	if got := string(first.Body); got != "Xoobar" {
		t.Fatalf("expected clones to not share bodies, first clone body changed to: %q", got)
	}
}
