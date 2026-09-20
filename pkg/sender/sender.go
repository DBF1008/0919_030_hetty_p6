package sender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

// Note: the client has no `Timeout` of its own; request timeouts are
// enforced per attempt via `RetryConfig.PerAttemptTimeout`.
var defaultHTTPClient = &http.Client{
	Transport: &HTTPTransport{},
}

var (
	ErrProjectIDMustBeSet = errors.New("sender: project ID must be set")
	ErrRequestNotFound    = errors.New("sender: request not found")
)

type Service struct {
	activeProjectID ulid.ULID
	findReqsFilter  FindRequestsFilter
	scope           *scope.Scope
	repo            Repository
	reqLogSvc       *reqlog.Service
	httpClient      *http.Client
	retryConfig     RetryConfig
	pendingStore    PendingResponseStore
}

type FindRequestsFilter struct {
	ProjectID   ulid.ULID
	OnlyInScope bool
	SearchExpr  filter.Expression
}

type Config struct {
	Scope         *scope.Scope
	Repository    Repository
	ReqLogService *reqlog.Service
	HTTPClient    *http.Client
	// Retry controls retry behavior and per-attempt timeouts for outgoing
	// requests. If nil, `DefaultRetryConfig` is used.
	Retry *RetryConfig
	// PendingResponseStore persists responses that couldn't be written to
	// the repository, so response data isn't lost on repository failures.
	PendingResponseStore PendingResponseStore
}

type SendError struct {
	err error
}

func NewService(cfg Config) *Service {
	svc := &Service{
		repo:         cfg.Repository,
		reqLogSvc:    cfg.ReqLogService,
		httpClient:   defaultHTTPClient,
		scope:        cfg.Scope,
		retryConfig:  DefaultRetryConfig(),
		pendingStore: cfg.PendingResponseStore,
	}

	if cfg.HTTPClient != nil {
		svc.httpClient = cfg.HTTPClient
	}

	if cfg.Retry != nil {
		svc.retryConfig = *cfg.Retry
	}

	return svc
}

type Request struct {
	ID                 ulid.ULID
	ProjectID          ulid.ULID
	SourceRequestLogID ulid.ULID

	URL    *url.URL
	Method string
	Proto  string
	Header http.Header
	Body   []byte

	Response *reqlog.ResponseLog
}

func (svc *Service) FindRequestByID(ctx context.Context, id ulid.ULID) (Request, error) {
	req, err := svc.repo.FindSenderRequestByID(ctx, svc.activeProjectID, id)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to find request: %w", err)
	}

	return req, nil
}

func (svc *Service) FindRequests(ctx context.Context) ([]Request, error) {
	return svc.repo.FindSenderRequests(ctx, svc.findReqsFilter, svc.scope)
}

func (svc *Service) CreateOrUpdateRequest(ctx context.Context, req Request) (Request, error) {
	if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
		return Request{}, ErrProjectIDMustBeSet
	}

	if req.ID.Compare(ulid.ULID{}) == 0 {
		req.ID = ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	}

	req.ProjectID = svc.activeProjectID

	if req.Method == "" {
		req.Method = http.MethodGet
	}

	if req.Proto == "" {
		req.Proto = HTTPProto20
	}

	if !isValidProto(req.Proto) {
		return Request{}, fmt.Errorf("sender: unsupported HTTP protocol: %v", req.Proto)
	}

	err := svc.repo.StoreSenderRequest(ctx, req)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to store request: %w", err)
	}

	return req, nil
}

func (svc *Service) CloneFromRequestLog(ctx context.Context, reqLogID ulid.ULID) (Request, error) {
	if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
		return Request{}, ErrProjectIDMustBeSet
	}

	reqLog, err := svc.reqLogSvc.FindRequestLogByID(ctx, reqLogID)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to find request log: %w", err)
	}

	req := Request{
		ID:                 ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		ProjectID:          svc.activeProjectID,
		SourceRequestLogID: reqLogID,
		Method:             reqLog.Method,
		URL:                reqLog.URL,
		Proto:              HTTPProto20, // Attempt HTTP/2.
		Header:             reqLog.Header.Clone(),
		Body:               append([]byte(nil), reqLog.Body...),
	}

	err = svc.repo.StoreSenderRequest(ctx, req)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to store request: %w", err)
	}

	return req, nil
}

func (svc *Service) SetFindReqsFilter(filter FindRequestsFilter) {
	svc.findReqsFilter = filter
}

func (svc *Service) FindReqsFilter() FindRequestsFilter {
	return svc.findReqsFilter
}

func (svc *Service) SendRequest(ctx context.Context, id ulid.ULID) (Request, error) {
	req, err := svc.repo.FindSenderRequestByID(ctx, svc.activeProjectID, id)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to find request: %w", err)
	}

	httpReq, err := parseHTTPRequest(ctx, req)
	if err != nil {
		return Request{}, fmt.Errorf("sender: failed to parse HTTP request: %w", err)
	}

	resLog, err := svc.sendHTTPRequest(ctx, httpReq)
	if err != nil {
		return Request{}, fmt.Errorf("sender: could not send HTTP request: %w", err)
	}

	req.Response = &resLog

	err = svc.repo.StoreSenderRequest(ctx, req)
	if err != nil {
		// The response was received but couldn't be stored; persist it to
		// the pending store (if configured) so the data isn't lost and can
		// be flushed later via `FlushPendingResponses`.
		if svc.pendingStore != nil {
			if storeErr := svc.pendingStore.StorePendingResponse(ctx, req); storeErr != nil {
				return Request{}, fmt.Errorf("sender: failed to store sender response log: %w "+
					"(additionally, persisting to pending store failed: %v)", err, storeErr)
			}

			return req, fmt.Errorf("sender: failed to store sender response log: %w: %v",
				ErrResponsePersisted, err)
		}

		return Request{}, fmt.Errorf("sender: failed to store sender response log: %w", err)
	}

	return req, nil
}

// FlushPendingResponses retries storing requests whose responses were
// persisted to the pending store after a repository failure. Successfully
// stored requests are removed from the pending store. It returns the IDs of
// the requests that are still pending.
func (svc *Service) FlushPendingResponses(ctx context.Context) ([]ulid.ULID, error) {
	if svc.pendingStore == nil {
		return nil, nil
	}

	pending, err := svc.pendingStore.PendingResponses(ctx)
	if err != nil {
		return nil, fmt.Errorf("sender: failed to list pending responses: %w", err)
	}

	var stillPending []ulid.ULID

	for _, req := range pending {
		if err := svc.repo.StoreSenderRequest(ctx, req); err != nil {
			stillPending = append(stillPending, req.ID)
			continue
		}

		if err := svc.pendingStore.DeletePendingResponse(ctx, req.ID); err != nil {
			return stillPending, fmt.Errorf("sender: failed to delete pending response: %w", err)
		}
	}

	return stillPending, nil
}

func parseHTTPRequest(ctx context.Context, req Request) (*http.Request, error) {
	ctx = context.WithValue(ctx, protoCtxKey{}, req.Proto)

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL.String(), bytes.NewReader(req.Body))
	if err != nil {
		return nil, fmt.Errorf("failed to construct HTTP request: %w", err)
	}

	if req.Header != nil {
		httpReq.Header = req.Header
	}

	return httpReq, nil
}

func (svc *Service) SetActiveProjectID(id ulid.ULID) {
	svc.activeProjectID = id
}

func (svc *Service) DeleteRequests(ctx context.Context, projectID ulid.ULID) error {
	return svc.repo.DeleteSenderRequests(ctx, projectID)
}

func (e SendError) Error() string {
	return fmt.Sprintf("failed to send HTTP request: %v", e.err)
}

func (e SendError) Unwrap() error {
	return e.err
}
