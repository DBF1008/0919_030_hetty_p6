package sender_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/sender"
)

func TestSendRequestStoreFailurePersistsResponse(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return stubResponse(http.StatusOK, "response body"), nil
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)

	pendingStore, err := sender.NewFilePendingResponseStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create pending response store: %v", err)
	}

	svc := sender.NewService(sender.Config{
		Repository:           repo,
		HTTPClient:           client,
		Retry:                fastRetryConfig(1),
		PendingResponseStore: pendingStore,
	})
	svc.SetActiveProjectID(projectID)

	// Simulate a repository failure: the response is received, but can't be
	// stored in the repository.
	repo.setStoreErr(errFakeStore)

	_, err = svc.SendRequest(context.Background(), reqID)
	if !errors.Is(err, sender.ErrResponsePersisted) {
		t.Fatalf("expected `sender.ErrResponsePersisted`, got: %v", err)
	}

	// The response must be persisted in the pending store.
	pending, err := pendingStore.PendingResponses(context.Background())
	if err != nil {
		t.Fatalf("failed to list pending responses: %v", err)
	}

	if len(pending) != 1 {
		t.Fatalf("expected 1 pending response, got: %d", len(pending))
	}

	if pending[0].ID != reqID {
		t.Fatalf("expected pending request ID %v, got: %v", reqID, pending[0].ID)
	}

	if pending[0].Response == nil || string(pending[0].Response.Body) != "response body" {
		t.Fatalf("expected pending response body %q, got: %+v", "response body", pending[0].Response)
	}

	// Recover the repository and flush the pending responses.
	repo.setStoreErr(nil)

	stillPending, err := svc.FlushPendingResponses(context.Background())
	if err != nil {
		t.Fatalf("unexpected error flushing pending responses: %v", err)
	}

	if len(stillPending) != 0 {
		t.Fatalf("expected no pending responses after flush, got: %v", stillPending)
	}

	got, err := repo.FindSenderRequestByID(context.Background(), projectID, reqID)
	if err != nil {
		t.Fatalf("failed to find request: %v", err)
	}

	if got.Response == nil || string(got.Response.Body) != "response body" {
		t.Fatalf("expected flushed response body %q, got: %+v", "response body", got.Response)
	}

	pending, err = pendingStore.PendingResponses(context.Background())
	if err != nil {
		t.Fatalf("failed to list pending responses: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected pending store to be empty after flush, got: %d entries", len(pending))
	}
}

func TestSendRequestStoreFailureWithoutPendingStore(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return stubResponse(http.StatusOK, "response body"), nil
		}),
	}

	repo := newFakeRepository()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	reqID := storeTestRequest(t, repo, projectID, exampleURL)

	svc := sender.NewService(sender.Config{
		Repository: repo,
		HTTPClient: client,
		Retry:      fastRetryConfig(1),
	})
	svc.SetActiveProjectID(projectID)

	repo.setStoreErr(errFakeStore)

	_, err := svc.SendRequest(context.Background(), reqID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if errors.Is(err, sender.ErrResponsePersisted) {
		t.Fatalf("expected no `sender.ErrResponsePersisted` without pending store, got: %v", err)
	}
}

func TestFilePendingResponseStoreRoundTrip(t *testing.T) {
	t.Parallel()

	store, err := sender.NewFilePendingResponseStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create pending response store: %v", err)
	}

	ctx := context.Background()
	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)

	req := sender.Request{
		ID:        ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		ProjectID: projectID,
		URL:       exampleURL,
		Method:    http.MethodPost,
		Proto:     sender.HTTPProto11,
		Header: http.Header{
			"X-Foo": []string{"bar"},
		},
		Body: []byte("foobar"),
	}

	if err := store.StorePendingResponse(ctx, req); err != nil {
		t.Fatalf("failed to store pending response: %v", err)
	}

	pending, err := store.PendingResponses(ctx)
	if err != nil {
		t.Fatalf("failed to list pending responses: %v", err)
	}

	if len(pending) != 1 {
		t.Fatalf("expected 1 pending response, got: %d", len(pending))
	}

	got := pending[0]
	if got.ID != req.ID || got.ProjectID != req.ProjectID || got.URL.String() != req.URL.String() ||
		got.Method != req.Method || got.Proto != req.Proto || string(got.Body) != string(req.Body) ||
		got.Header.Get("X-Foo") != "bar" {
		t.Fatalf("pending request mismatch, got: %+v", got)
	}

	if err := store.DeletePendingResponse(ctx, req.ID); err != nil {
		t.Fatalf("failed to delete pending response: %v", err)
	}

	// Deleting a non-existent entry must not fail.
	if err := store.DeletePendingResponse(ctx, req.ID); err != nil {
		t.Fatalf("expected deleting missing entry to be a no-op, got: %v", err)
	}

	pending, err = store.PendingResponses(ctx)
	if err != nil {
		t.Fatalf("failed to list pending responses: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected no pending responses, got: %d", len(pending))
	}
}
