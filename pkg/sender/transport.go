package sender

import (
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"
)

type protoCtxKey struct{}

const (
	HTTPProto10 = "HTTP/1.0"
	HTTPProto11 = "HTTP/1.1"
	HTTPProto20 = "HTTP/2.0"
)

// Default connection pool settings, applied when the corresponding
// HTTPTransport field is left at its zero value.
const (
	defaultMaxIdleConns        = 100
	defaultMaxIdleConnsPerHost = 8
	defaultIdleConnTimeout     = 90 * time.Second
	defaultDialTimeout         = 30 * time.Second
	defaultDialKeepAlive       = 30 * time.Second
	defaultTLSHandshakeTimeout = 10 * time.Second
)

// HTTPTransport is an http.RoundTripper that switches between HTTP/1.x and
// HTTP/2 based on a context value on the outgoing request. Its connection
// pool settings are configurable; zero values fall back to sane defaults.
type HTTPTransport struct {
	// MaxConnsPerHost optionally limits the total number of connections per
	// host, including connections in the dialing, active, and idle states.
	// Zero means no limit.
	MaxConnsPerHost int
	// MaxIdleConns controls the maximum number of idle (keep-alive)
	// connections across all hosts. Zero means 100.
	MaxIdleConns int
	// MaxIdleConnsPerHost controls the maximum number of idle (keep-alive)
	// connections per host. Zero means 8.
	MaxIdleConnsPerHost int
	// IdleConnTimeout is the maximum amount of time an idle (keep-alive)
	// connection remains idle before closing itself. Zero means 90 seconds.
	IdleConnTimeout time.Duration

	once sync.Once
	h1   http.RoundTripper
	h2   http.RoundTripper
}

func (t *HTTPTransport) maxIdleConns() int {
	if t.MaxIdleConns > 0 {
		return t.MaxIdleConns
	}

	return defaultMaxIdleConns
}

func (t *HTTPTransport) maxIdleConnsPerHost() int {
	if t.MaxIdleConnsPerHost > 0 {
		return t.MaxIdleConnsPerHost
	}

	return defaultMaxIdleConnsPerHost
}

func (t *HTTPTransport) idleConnTimeout() time.Duration {
	if t.IdleConnTimeout > 0 {
		return t.IdleConnTimeout
	}

	return defaultIdleConnTimeout
}

// init lazily builds the underlying HTTP/1.x and HTTP/2 transports, so a
// shared HTTPTransport value remains safe for concurrent use.
func (t *HTTPTransport) init() {
	t.once.Do(func() {
		base := func() *http.Transport {
			return &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   defaultDialTimeout,
					KeepAlive: defaultDialKeepAlive,
				}).DialContext,
				MaxConnsPerHost:       t.MaxConnsPerHost,
				MaxIdleConns:          t.maxIdleConns(),
				MaxIdleConnsPerHost:   t.maxIdleConnsPerHost(),
				IdleConnTimeout:       t.idleConnTimeout(),
				TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
				ExpectContinueTimeout: 1 * time.Second,
			}
		}

		// HTTP/1.x only transport: mimics `http.DefaultTransport`, but with
		// HTTP/2 disabled.
		h1 := base()
		h1.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		t.h1 = h1

		// HTTP/2 capable transport: a nil TLSNextProto map lets the net/http
		// package automatically negotiate HTTP/2 over TLS.
		t.h2 = base()
	})
}

// RoundTrip implements http.RoundTripper. Based on a context value on the
// HTTP request, it switches between an HTTP/2 capable transport and an
// HTTP/1.x only transport. Both transports use bounded connection pools, so
// a burst of sender requests cannot exhaust file descriptors.
func (t *HTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.init()

	proto, ok := req.Context().Value(protoCtxKey{}).(string)
	if ok && (proto == HTTPProto10 || proto == HTTPProto11) {
		return t.h1.RoundTrip(req)
	}

	return t.h2.RoundTrip(req)
}

func isValidProto(proto string) bool {
	return proto == HTTPProto10 || proto == HTTPProto11 || proto == HTTPProto20
}
