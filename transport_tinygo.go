//go:build tinygo

package nexus

import (
	"fmt"
	"net/http"
)

func protectedTransport(base http.RoundTripper, policy addressPolicy) (http.RoundTripper, error) {
	// TinyGo transports bypass the dialer used for connection-time address checks.
	if !policy.allowPrivate {
		return nil, fmt.Errorf("%w: TinyGo requires AllowPrivateAddresses", ErrUnprotectedTransport)
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &checkingRoundTripper{
		base:                base,
		policy:              policy,
		responseIdleTimeout: policy.responseIdleTimeout,
	}, nil
}
