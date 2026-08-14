package nexus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientRejectsUnsafeRepositoryURLs(t *testing.T) {
	tests := []string{
		"ftp://repo.example.test/releases",
		"https://user:secret@repo.example.test/releases",
		"https://repo.example.test/releases?view=index",
		"https://repo.example.test/releases#fragment",
		"https:///releases",
	}
	client := NewClient(ClientOptions{})
	for _, remote := range tests {
		t.Run(remote, func(t *testing.T) {
			_, err := client.Sync(context.Background(), remote, nil)
			if !errors.Is(err, ErrUnsafeURL) {
				t.Fatalf("Sync error = %v, want ErrUnsafeURL", err)
			}
		})
	}
}

func TestClientRejectsInvalidOptionsBeforeRequest(t *testing.T) {
	tests := []ClientOptions{
		{MaxPropertiesBytes: -1},
		{MaxCompressedBytes: -1},
		{MaxRedirects: -1},
		{MaxRetries: -1},
		{RetryBaseDelay: -1},
		{MaxRetryDelay: -1},
		{ResponseIdleTimeout: -1},
	}
	for _, options := range tests {
		client := NewClient(options)
		if _, err := client.Sync(context.Background(), "https://8.8.8.8", nil); err == nil {
			t.Errorf("NewClient(%+v) allowed invalid options", options)
		}
	}
}

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

func TestClientAllowsCustomTransportWithPrivateAddresses(t *testing.T) {
	var calls atomic.Int32
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})
	client := NewClient(ClientOptions{
		HTTPClient:            &http.Client{Transport: transport},
		AllowPrivateAddresses: true,
	})
	_, err := client.Sync(context.Background(), "http://repo.example.test/repository", nil)
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("Sync error = %v, want ErrIndexNotFound", err)
	}
	if calls.Load() != 1 {
		t.Errorf("transport calls = %d, want 1", calls.Load())
	}
}

func TestClientChecksRedirectPolicy(t *testing.T) {
	t.Run("credentials", func(t *testing.T) {
		var calls atomic.Int32
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			remote := "http://user:secret@" + strings.TrimPrefix(server.URL, "http://")
			http.Redirect(writer, &http.Request{}, remote, http.StatusFound)
		}))
		defer server.Close()

		client := NewClient(ClientOptions{AllowPrivateAddresses: true})
		_, err := client.Sync(context.Background(), server.URL, nil)
		if !errors.Is(err, ErrUnsafeURL) {
			t.Fatalf("Sync error = %v, want ErrUnsafeURL", err)
		}
		if calls.Load() != 1 {
			t.Errorf("requests = %d, want 1", calls.Load())
		}
	})

	t.Run("limit", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			calls.Add(1)
			http.Redirect(writer, request, "/next", http.StatusFound)
		}))
		defer server.Close()

		client := NewClient(ClientOptions{AllowPrivateAddresses: true, MaxRedirects: 2})
		_, err := client.Sync(context.Background(), server.URL, nil)
		if !errors.Is(err, ErrUnsafeURL) {
			t.Fatalf("Sync error = %v, want ErrUnsafeURL", err)
		}
		if calls.Load() != 2 {
			t.Errorf("requests = %d, want 2", calls.Load())
		}
	})

	t.Run("private address", func(t *testing.T) {
		var calls atomic.Int32
		base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     http.Header{"Location": []string{"http://127.0.0.1/private"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		})
		httpClient := &http.Client{Transport: &checkingRoundTripper{base: base, policy: addressPolicy{}}}
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://8.8.8.8/index", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = httpClient.Do(request)
		if !errors.Is(err, ErrPrivateAddress) {
			t.Fatalf("Do error = %v, want ErrPrivateAddress", err)
		}
		if calls.Load() != 1 {
			t.Errorf("transport calls = %d, want 1", calls.Load())
		}
	})
}

func TestClientHonorsRetryAfterCancellation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.Header().Set("Retry-After", "60")
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := NewClient(ClientOptions{AllowPrivateAddresses: true, MaxRetries: 1})
	started := time.Now()
	_, err := client.Sync(ctx, server.URL, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sync error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("cancellation took %s", elapsed)
	}
	if calls.Load() != 1 {
		t.Errorf("requests = %d, want 1", calls.Load())
	}
}

func TestParseRetryAfter(t *testing.T) {
	delay, ok := parseRetryAfter("5")
	if !ok || delay != 5*time.Second {
		t.Errorf("parseRetryAfter(5) = %s, %v", delay, ok)
	}
	if delay, ok := parseRetryAfter("9223372036854775807"); !ok || delay != time.Duration(1<<63-1) {
		t.Errorf("overflow Retry-After = %s, %v", delay, ok)
	}
	if _, ok := parseRetryAfter("invalid"); ok {
		t.Error("invalid Retry-After was accepted")
	}
}

func TestCheckPublicIP(t *testing.T) {
	tests := []struct {
		address string
		blocked bool
	}{
		{"8.8.8.8", false},
		{"0.0.0.0", true},
		{"127.0.0.1", true},
		{"10.0.0.1", true},
		{"169.254.1.1", true},
		{"224.0.0.1", true},
		{"100.64.0.1", true},
		{"::1", true},
		{"fe80::1", true},
		{"64:ff9b::a9fe:a9fe", true},
		{"64:ff9b:1::1", true},
	}
	for _, test := range tests {
		err := checkPublicIP(net.ParseIP(test.address))
		if test.blocked != errors.Is(err, ErrPrivateAddress) {
			t.Errorf("checkPublicIP(%s) error = %v", test.address, err)
		}
	}
}

func TestClientResponseIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Length", "128")
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		<-release
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := NewClient(ClientOptions{
		AllowPrivateAddresses: true,
		ResponseIdleTimeout:   20 * time.Millisecond,
	})
	started := time.Now()
	_, err := client.Sync(ctx, server.URL, nil)
	close(release)
	var networkErr net.Error
	if !errors.As(err, &networkErr) || !networkErr.Timeout() {
		t.Fatalf("Sync error = %v, want network timeout", err)
	}
	if !errors.Is(err, ErrResponseIdleTimeout) {
		t.Fatalf("Sync error = %v, want ErrResponseIdleTimeout", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sync error = %v, response idle timeout did not fire", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Errorf("idle timeout took %s", elapsed)
	}
}

func TestMaxReadCloserWithoutContentLength(t *testing.T) {
	input := &trackingReader{Reader: bytes.NewReader([]byte("1234"))}
	reader := &maxReadCloser{reader: input, remaining: 3, limit: 3}
	data, err := io.ReadAll(reader)
	if string(data) != "123" || !errors.Is(err, ErrCompressedLimit) {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !input.closed {
		t.Error("Close did not close the response body")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
