package sender_test

import (
	"context"
	"errors"
	"sync"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

// fakeRepository is an in-memory `sender.Repository` implementation for
// tests. `storeErr` can be set to simulate repository failures.
type fakeRepository struct {
	mu       sync.Mutex
	requests map[ulid.ULID]sender.Request
	storeErr error
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{
		requests: make(map[ulid.ULID]sender.Request),
	}
}

func (r *fakeRepository) FindSenderRequestByID(_ context.Context, projectID, id ulid.ULID) (sender.Request, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	req, ok := r.requests[id]
	if !ok || req.ProjectID != projectID {
		return sender.Request{}, sender.ErrRequestNotFound
	}

	return req, nil
}

func (r *fakeRepository) FindSenderRequests(_ context.Context, _ sender.FindRequestsFilter, _ *scope.Scope) ([]sender.Request, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reqs := make([]sender.Request, 0, len(r.requests))
	for _, req := range r.requests {
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *fakeRepository) StoreSenderRequest(_ context.Context, req sender.Request) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.storeErr != nil {
		return r.storeErr
	}

	r.requests[req.ID] = req

	return nil
}

func (r *fakeRepository) DeleteSenderRequests(_ context.Context, projectID ulid.ULID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, req := range r.requests {
		if req.ProjectID == projectID {
			delete(r.requests, id)
		}
	}

	return nil
}

func (r *fakeRepository) setStoreErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.storeErr = err
}

var errFakeStore = errors.New("fake repository: store failure")
