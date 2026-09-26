//go:build !tinygo

package nexus

import (
	"context"
	"fmt"
	"net"
	"net/http"
)

func protectedTransport(base http.RoundTripper, policy addressPolicy) (http.RoundTripper, error) {
	usingDefault := base == nil
	defaultTransport, defaultIsHTTP := http.DefaultTransport.(*http.Transport)
	if transport, ok := base.(*http.Transport); ok && defaultIsHTTP && transport == defaultTransport {
		usingDefault = true
	}
	if base == nil {
		base = http.DefaultTransport
	}
	if transport, ok := base.(*http.Transport); ok {
		clone := transport.Clone()
		if !policy.allowPrivate {
			if !usingDefault && clone.Proxy != nil {
				return nil, fmt.Errorf("%w: proxies require AllowPrivateAddresses", ErrUnprotectedTransport)
			}
			if !usingDefault && clone.DialContext != nil {
				return nil, fmt.Errorf("%w: custom dialers require AllowPrivateAddresses", ErrUnprotectedTransport)
			}
			hasCustomTLSDialer := clone.DialTLSContext != nil
			hasCustomTLSDialer = hasCustomTLSDialer || clone.DialTLS != nil //nolint:staticcheck // DialTLS remains supported and bypasses DialContext.
			if hasCustomTLSDialer {
				return nil, fmt.Errorf("%w: custom TLS dialers require AllowPrivateAddresses", ErrUnprotectedTransport)
			}
			clone.Proxy = nil
		}
		if clone.ResponseHeaderTimeout == 0 {
			clone.ResponseHeaderTimeout = defaultHeaderTimeout
		}
		underlying := clone.DialContext
		if !policy.allowPrivate || underlying == nil {
			dialer := &net.Dialer{Timeout: defaultDialTimeout, KeepAlive: defaultKeepAlive}
			underlying = dialer.DialContext
		}
		clone.DialContext = policy.dialContext(underlying)
		base = clone
	} else if !policy.allowPrivate {
		return nil, fmt.Errorf("%w: custom RoundTripper requires AllowPrivateAddresses", ErrUnprotectedTransport)
	}
	return &checkingRoundTripper{
		base:                base,
		policy:              policy,
		responseIdleTimeout: policy.responseIdleTimeout,
	}, nil
}

func (policy addressPolicy) dialContext(underlying func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if policy.allowPrivate {
			return underlying(ctx, network, address)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if parsed := net.ParseIP(host); parsed != nil {
			if err := checkPublicIP(parsed); err != nil {
				return nil, err
			}
			return underlying(ctx, network, address)
		}
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, candidate := range addresses {
			if err := checkPublicIP(candidate.IP); err != nil {
				return nil, fmt.Errorf("nexus: host %s: %w", host, err)
			}
			connection, dialErr := underlying(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			lastErr = dialErr
		}
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("nexus: no addresses resolved for %s", host)
	}
}
