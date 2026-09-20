package sender

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/reqlog"
)

// ErrResponsePersisted is returned (wrapped) by `SendRequest` when the HTTP
// response was received but couldn't be stored in the repository. The
// response is persisted in the configured `PendingResponseStore` instead, so
// it can be flushed later via `FlushPendingResponses`.
var ErrResponsePersisted = errors.New("sender: response persisted to pending store after repository failure")

// PendingResponseStore persists sender requests (including their responses)
// that couldn't be written to the primary repository, so response data isn't
// lost when the repository is (temporarily) unavailable.
type PendingResponseStore interface {
	StorePendingResponse(ctx context.Context, req Request) error
	PendingResponses(ctx context.Context) ([]Request, error)
	DeletePendingResponse(ctx context.Context, id ulid.ULID) error
}

// FilePendingResponseStore is a `PendingResponseStore` that persists pending
// responses as JSON files (one per request) in a directory on disk.
type FilePendingResponseStore struct {
	dir string
	mu  sync.Mutex
}

// NewFilePendingResponseStore creates a file based pending response store
// rooted at dir, creating the directory if needed.
func NewFilePendingResponseStore(dir string) (*FilePendingResponseStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("sender: failed to create pending response dir: %w", err)
	}

	return &FilePendingResponseStore{dir: dir}, nil
}

type pendingResponseFile struct {
	ID                 ulid.ULID           `json:"id"`
	ProjectID          ulid.ULID           `json:"projectId"`
	SourceRequestLogID ulid.ULID           `json:"sourceRequestLogId"`
	URL                string              `json:"url"`
	Method             string              `json:"method"`
	Proto              string              `json:"proto"`
	Header             http.Header         `json:"header"`
	Body               []byte              `json:"body"`
	Response           *reqlog.ResponseLog `json:"response,omitempty"`
}

func requestToFile(req Request) pendingResponseFile {
	var rawURL string
	if req.URL != nil {
		rawURL = req.URL.String()
	}

	return pendingResponseFile{
		ID:                 req.ID,
		ProjectID:          req.ProjectID,
		SourceRequestLogID: req.SourceRequestLogID,
		URL:                rawURL,
		Method:             req.Method,
		Proto:              req.Proto,
		Header:             req.Header,
		Body:               req.Body,
		Response:           req.Response,
	}
}

func (f pendingResponseFile) toRequest() (Request, error) {
	parsedURL, err := url.Parse(f.URL)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to parse pending request URL: %w", err)
	}

	return Request{
		ID:                 f.ID,
		ProjectID:          f.ProjectID,
		SourceRequestLogID: f.SourceRequestLogID,
		URL:                parsedURL,
		Method:             f.Method,
		Proto:              f.Proto,
		Header:             f.Header,
		Body:               f.Body,
		Response:           f.Response,
	}, nil
}

func (s *FilePendingResponseStore) path(id ulid.ULID) string {
	return filepath.Join(s.dir, id.String()+".json")
}

func (s *FilePendingResponseStore) StorePendingResponse(_ context.Context, req Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(requestToFile(req))
	if err != nil {
		return fmt.Errorf("sender: failed to encode pending response: %w", err)
	}

	if err := os.WriteFile(s.path(req.ID), data, 0o600); err != nil {
		return fmt.Errorf("sender: failed to write pending response file: %w", err)
	}

	return nil
}

func (s *FilePendingResponseStore) PendingResponses(_ context.Context) ([]Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("sender: failed to read pending response dir: %w", err)
	}

	var reqs []Request

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("sender: failed to read pending response file: %w", err)
		}

		var f pendingResponseFile
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, fmt.Errorf("sender: failed to decode pending response file %q: %w", entry.Name(), err)
		}

		req, err := f.toRequest()
		if err != nil {
			return nil, err
		}

		reqs = append(reqs, req)
	}

	sort.Slice(reqs, func(i, j int) bool {
		return reqs[i].ID.Compare(reqs[j].ID) < 0
	})

	return reqs, nil
}

func (s *FilePendingResponseStore) DeletePendingResponse(_ context.Context, id ulid.ULID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("sender: failed to delete pending response file: %w", err)
	}

	return nil
}
