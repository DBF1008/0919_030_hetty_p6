package sender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

var defaultHTTPClient = &http.Client{
	Transport: &HTTPTransport{},
	Timeout:   30 * time.Second,
}

var (
	ErrProjectIDMustBeSet = errors.New("sender: project ID must be set")
	ErrRequestNotFound    = errors.New("sender: request not found")
	// ErrResponseNotStored is returned (wrapped) when a response was received
	// from the target server, but persisting it to the repository failed. The
	// returned Request still carries the response, so no data is lost.
	ErrResponseNotStored = errors.New("sender: response received but could not be stored")
)

// Defaults for persisting sender requests. Storing is retried a few times so
// a transient repository error doesn't silently drop a received response.
const (
	storeMaxAttempts = 3
	storeBackoff     = 100 * time.Millisecond
)

// DefaultRetryableStatusCodes are HTTP response status codes that trigger a
// retry when retries are enabled.
var DefaultRetryableStatusCodes = []int{
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
}

// RetryConfig controls how SendRequest retries failed attempts. The zero
// value disables retries (a single attempt), preserving the historical
// behavior.
type RetryConfig struct {
	// MaxAttempts is the total number of send attempts, including the first.
	// Values <= 1 disable retries.
	MaxAttempts int
	// InitialBackoff is the wait time before the first retry. Zero means
	// 100 milliseconds.
	InitialBackoff time.Duration
	// MaxBackoff caps the wait time between attempts. Zero means 2 seconds.
	MaxBackoff time.Duration
	// Multiplier scales the backoff after each failed attempt. Zero means 2.
	Multiplier float64
	// AttemptTimeout optionally limits the duration of each individual
	// attempt. Zero means no per-attempt timeout (the http.Client timeout
	// still applies).
	AttemptTimeout time.Duration
	// RetryableStatusCodes lists HTTP status codes that trigger a retry.
	// Nil means DefaultRetryableStatusCodes.
	RetryableStatusCodes []int
}

func (cfg RetryConfig) withDefaults() RetryConfig {
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 100 * time.Millisecond
	}

	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Second
	}

	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2
	}

	if cfg.RetryableStatusCodes == nil {
		cfg.RetryableStatusCodes = DefaultRetryableStatusCodes
	}

	return cfg
}

func (cfg RetryConfig) retryableStatus(code int) bool {
	for _, c := range cfg.RetryableStatusCodes {
		if c == code {
			return true
		}
	}

	return false
}

type Service struct {
	activeProjectID ulid.ULID
	findReqsFilter  FindRequestsFilter
	scope           *scope.Scope
	repo            Repository
	reqLogSvc       *reqlog.Service
	httpClient      *http.Client
	retry           RetryConfig
	backoffRand     *rand.Rand
	backoffRandMu   sync.Mutex
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
	// Retry optionally configures retries for SendRequest. The zero value
	// disables retries.
	Retry RetryConfig
}

type SendError struct {
	err error
}

func NewService(cfg Config) *Service {
	svc := &Service{
		repo:        cfg.Repository,
		reqLogSvc:   cfg.ReqLogService,
		httpClient:  defaultHTTPClient,
		scope:       cfg.Scope,
		retry:       cfg.Retry,
		backoffRand: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
	}

	if cfg.HTTPClient != nil {
		svc.httpClient = cfg.HTTPClient
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
		// Deep-copy header and body, so the clone never aliases (and can
		// never be emptied by consumers of) the original request log.
		Header: reqLog.Header.Clone(),
		Body:   bytes.Clone(reqLog.Body),
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

	// Persist the request with its response. Storing is retried a few times
	// (compensation), and if it still fails the request — including the
	// received response — is returned along with ErrResponseNotStored, so
	// the response data is never silently lost.
	if err := svc.storeRequest(ctx, req); err != nil {
		return req, fmt.Errorf("sender: %w: %v", ErrResponseNotStored, err)
	}

	return req, nil
}

// storeRequest persists req, retrying transient repository failures.
func (svc *Service) storeRequest(ctx context.Context, req Request) error {
	var err error

	for attempt := 1; attempt <= storeMaxAttempts; attempt++ {
		if attempt > 1 {
			timer := time.NewTimer(storeBackoff * time.Duration(attempt-1))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		if err = svc.repo.StoreSenderRequest(ctx, req); err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to store sender response log after %d attempts: %w", storeMaxAttempts, err)
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

// sendHTTPRequest sends httpReq, retrying failed attempts according to the
// service's RetryConfig. Transport errors and responses with a retryable
// status code are retried with exponential backoff and jitter. If retries
// are exhausted on a retryable status code, the last received response is
// returned.
func (svc *Service) sendHTTPRequest(ctx context.Context, httpReq *http.Request) (reqlog.ResponseLog, error) {
	cfg := svc.retry.withDefaults()

	attempts := cfg.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	var (
		resLog  reqlog.ResponseLog
		lastErr error
	)

	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := svc.waitBackoff(ctx, cfg, attempt-1); err != nil {
				return reqlog.ResponseLog{}, err
			}
		}

		attemptCtx := ctx
		cancel := context.CancelFunc(func() {})

		if cfg.AttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, cfg.AttemptTimeout)
		}

		resLog, lastErr = svc.doAttempt(attemptCtx, httpReq)

		cancel()

		if lastErr != nil {
			// Don't retry if the caller's context is done.
			if ctx.Err() != nil {
				return reqlog.ResponseLog{}, lastErr
			}

			continue
		}

		if !cfg.retryableStatus(resLog.StatusCode) || attempt == attempts {
			return resLog, nil
		}
	}

	return reqlog.ResponseLog{}, lastErr
}

// doAttempt performs a single send attempt. The request is cloned and its
// body reset (via GetBody) so every attempt sends the full payload.
func (svc *Service) doAttempt(ctx context.Context, httpReq *http.Request) (reqlog.ResponseLog, error) {
	req := httpReq.Clone(ctx)

	if httpReq.GetBody != nil {
		body, err := httpReq.GetBody()
		if err != nil {
			return reqlog.ResponseLog{}, fmt.Errorf("failed to reset request body: %w", err)
		}

		req.Body = body
	}

	res, err := svc.httpClient.Do(req)
	if err != nil {
		return reqlog.ResponseLog{}, &SendError{err}
	}
	defer res.Body.Close()

	resLog, err := reqlog.ParseHTTPResponse(res)
	if err != nil {
		return reqlog.ResponseLog{}, fmt.Errorf("failed to parse http response: %w", err)
	}

	return resLog, nil
}

// waitBackoff sleeps for an exponentially increasing (and jittered) duration
// before the next retry, returning early if ctx is done.
func (svc *Service) waitBackoff(ctx context.Context, cfg RetryConfig, retry int) error {
	backoff := float64(cfg.InitialBackoff)
	for i := 1; i < retry; i++ {
		backoff *= cfg.Multiplier
	}

	if backoff > float64(cfg.MaxBackoff) {
		backoff = float64(cfg.MaxBackoff)
	}

	svc.backoffRandMu.Lock()
	//nolint:gosec
	delay := time.Duration(backoff/2 + svc.backoffRand.Float64()*backoff/2)
	svc.backoffRandMu.Unlock()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
