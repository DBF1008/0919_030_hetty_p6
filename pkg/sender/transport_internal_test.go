package sender

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type recordRoundTripper struct {
	called *bool
}

func (rt recordRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	*rt.called = true
	return nil, errors.New("fake round tripper")
}

func TestHTTPTransportDefaultPoolSettings(t *testing.T) {
	t.Parallel()

	tr := &HTTPTransport{}
	tr.init()

	h1, ok := tr.h1.(*http.Transport)
	if !ok {
		t.Fatalf("expected h1 to be *http.Transport, got: %T", tr.h1)
	}

	h2, ok := tr.h2.(*http.Transport)
	if !ok {
		t.Fatalf("expected h2 to be *http.Transport, got: %T", tr.h2)
	}

	for name, transport := range map[string]*http.Transport{"h1": h1, "h2": h2} {
		if transport.MaxIdleConns != defaultMaxIdleConns {
			t.Errorf("%s: expected MaxIdleConns %d, got: %d", name, defaultMaxIdleConns, transport.MaxIdleConns)
		}

		if transport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
			t.Errorf("%s: expected MaxIdleConnsPerHost %d, got: %d", name, defaultMaxIdleConnsPerHost, transport.MaxIdleConnsPerHost)
		}

		if transport.IdleConnTimeout != defaultIdleConnTimeout {
			t.Errorf("%s: expected IdleConnTimeout %v, got: %v", name, defaultIdleConnTimeout, transport.IdleConnTimeout)
		}
	}

	// HTTP/1.x transport must have HTTP/2 disabled; HTTP/2 transport must not.
	if h1.TLSNextProto == nil {
		t.Error("expected h1 TLSNextProto to be non-nil (HTTP/2 disabled)")
	}

	if h2.TLSNextProto != nil {
		t.Error("expected h2 TLSNextProto to be nil (HTTP/2 enabled)")
	}
}

func TestHTTPTransportCustomPoolSettings(t *testing.T) {
	t.Parallel()

	tr := &HTTPTransport{
		MaxConnsPerHost:     4,
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     time.Second,
	}
	tr.init()

	for name, rt := range map[string]http.RoundTripper{"h1": tr.h1, "h2": tr.h2} {
		transport, ok := rt.(*http.Transport)
		if !ok {
			t.Fatalf("%s: expected *http.Transport, got: %T", name, rt)
		}

		if transport.MaxConnsPerHost != 4 {
			t.Errorf("%s: expected MaxConnsPerHost 4, got: %d", name, transport.MaxConnsPerHost)
		}

		if transport.MaxIdleConns != 10 {
			t.Errorf("%s: expected MaxIdleConns 10, got: %d", name, transport.MaxIdleConns)
		}

		if transport.MaxIdleConnsPerHost != 2 {
			t.Errorf("%s: expected MaxIdleConnsPerHost 2, got: %d", name, transport.MaxIdleConnsPerHost)
		}

		if transport.IdleConnTimeout != time.Second {
			t.Errorf("%s: expected IdleConnTimeout 1s, got: %v", name, transport.IdleConnTimeout)
		}
	}
}

func TestHTTPTransportProtoSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		proto    string
		setProto bool
		wantH1   bool
	}{
		{name: "http/1.0 uses h1", proto: HTTPProto10, setProto: true, wantH1: true},
		{name: "http/1.1 uses h1", proto: HTTPProto11, setProto: true, wantH1: true},
		{name: "http/2 uses h2", proto: HTTPProto20, setProto: true, wantH1: false},
		{name: "no proto defaults to h2", setProto: false, wantH1: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h1Called, h2Called bool

			tr := &HTTPTransport{
				h1: recordRoundTripper{called: &h1Called},
				h2: recordRoundTripper{called: &h2Called},
			}
			tr.once.Do(func() {}) // Prevent init from overwriting the fakes.

			ctx := context.Background()
			if tt.setProto {
				ctx = context.WithValue(ctx, protoCtxKey{}, tt.proto)
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			//nolint:bodyclose
			_, _ = tr.RoundTrip(req)

			if h1Called != tt.wantH1 {
				t.Errorf("expected h1 called: %v, got: %v", tt.wantH1, h1Called)
			}

			if h2Called == tt.wantH1 {
				t.Errorf("expected h2 called: %v, got: %v", !tt.wantH1, h2Called)
			}
		})
	}
}

func TestHTTPTransportConcurrentInit(t *testing.T) {
	t.Parallel()

	tr := &HTTPTransport{}

	done := make(chan struct{})

	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			tr.init()
		}()
	}

	for i := 0; i < 8; i++ {
		<-done
	}

	if tr.h1 == nil || tr.h2 == nil {
		t.Fatal("expected transports to be initialized")
	}
}
