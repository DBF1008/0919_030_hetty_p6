package sender

import "testing"

// TestTransportConnectionPoolLimits guards against unbounded connection
// pools, which can exhaust file descriptors under heavy sender load.
func TestTransportConnectionPoolLimits(t *testing.T) {
	t.Parallel()

	transports := map[string]struct {
		maxIdleConns        int
		maxIdleConnsPerHost int
		maxConnsPerHost     int
	}{
		"h1OnlyTransport": {
			h1OnlyTransport.MaxIdleConns,
			h1OnlyTransport.MaxIdleConnsPerHost,
			h1OnlyTransport.MaxConnsPerHost,
		},
		"h2Transport": {
			h2Transport.MaxIdleConns,
			h2Transport.MaxIdleConnsPerHost,
			h2Transport.MaxConnsPerHost,
		},
	}

	for name, tr := range transports {
		if tr.maxIdleConns <= 0 {
			t.Errorf("%s: MaxIdleConns must be > 0, got: %d", name, tr.maxIdleConns)
		}

		if tr.maxIdleConnsPerHost <= 0 {
			t.Errorf("%s: MaxIdleConnsPerHost must be > 0, got: %d", name, tr.maxIdleConnsPerHost)
		}

		if tr.maxConnsPerHost <= 0 {
			t.Errorf("%s: MaxConnsPerHost must be > 0, got: %d", name, tr.maxConnsPerHost)
		}
	}
}
