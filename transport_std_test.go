//go:build !tinygo

package nexus

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
)

func TestClientRejectsUnprotectedTransports(t *testing.T) {
	proxyURL := &url.URL{Scheme: "http", Host: "proxy.example.test"}
	tests := []struct {
		name      string
		transport http.RoundTripper
	}{
		{
			name: "custom RoundTripper",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("transport should not be called")
			}),
		},
		{
			name:      "proxy",
			transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
		},
		{
			name: "custom dialer",
			transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("dialer should not be called")
			}},
		},
		{
			name: "custom TLS dialer",
			transport: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("dialer should not be called")
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClient(ClientOptions{HTTPClient: &http.Client{Transport: test.transport}})
			_, err := client.Sync(context.Background(), "https://8.8.8.8/repository", nil)
			if !errors.Is(err, ErrUnprotectedTransport) {
				t.Fatalf("Sync error = %v, want ErrUnprotectedTransport", err)
			}
		})
	}
}
