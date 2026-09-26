package nexus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultMaxCompressedBytes permits a gzip chunk up to 8 GiB.
	DefaultMaxCompressedBytes int64 = 8 << 30
	// DefaultMaxRedirects bounds redirects for every request.
	DefaultMaxRedirects = 10
	// DefaultMaxRetries permits two retries after the initial request.
	DefaultMaxRetries = 2

	defaultRetryBaseDelay = 250 * time.Millisecond
	defaultMaxRetryDelay  = 30 * time.Second
	defaultDialTimeout    = 30 * time.Second
	defaultKeepAlive      = 30 * time.Second
	defaultHeaderTimeout  = 30 * time.Second
	defaultIdleTimeout    = 30 * time.Second
)

var (
	// ErrCompressedLimit is returned when a chunk exceeds its compressed-byte
	// limit.
	ErrCompressedLimit = errors.New("nexus: compressed chunk exceeds size limit")
	// ErrIndexNotFound is returned when a repository has no index properties.
	ErrIndexNotFound = errors.New("nexus: Maven index properties not found")
	// ErrPrivateAddress is returned when strict address policy blocks a host.
	ErrPrivateAddress = errors.New("nexus: refusing non-public address")
	// ErrResponseIdleTimeout is returned when a response body read receives no
	// data before the configured idle timeout.
	ErrResponseIdleTimeout = errors.New("nexus: response body idle timeout")
	// ErrUnsafeURL is returned when a URL violates the remote URL policy.
	ErrUnsafeURL = errors.New("nexus: remote URL rejected")
	// ErrUnprotectedTransport is returned when strict address policy cannot be
	// enforced by the HTTP transport or proxy.
	ErrUnprotectedTransport = errors.New("nexus: HTTP transport cannot enforce public-address policy")
)

// ClientOptions configures remote repository synchronization. The zero value
// uses safe defaults.
type ClientOptions struct {
	HTTPClient            *http.Client
	ParserOptions         Options
	MaxPropertiesBytes    int64
	MaxCompressedBytes    int64
	MaxRedirects          int
	MaxRetries            int
	RetryBaseDelay        time.Duration
	MaxRetryDelay         time.Duration
	ResponseIdleTimeout   time.Duration
	UserAgent             string
	AllowPrivateAddresses bool
}

type normalizedClientOptions struct {
	parserOptions         Options
	maxPropertiesBytes    int64
	maxCompressedBytes    int64
	maxRedirects          int
	maxRetries            int
	retryBaseDelay        time.Duration
	maxRetryDelay         time.Duration
	responseIdleTimeout   time.Duration
	userAgent             string
	allowPrivateAddresses bool
}

// Client downloads and streams Maven repository indexes.
type Client struct {
	http      *http.Client
	options   normalizedClientOptions
	configErr error
}

// HTTPStatusError reports a non-success response from an index endpoint.
type HTTPStatusError struct {
	StatusCode int
	URL        string
}

func (err *HTTPStatusError) Error() string {
	return fmt.Sprintf("nexus: GET %s returned HTTP %d", err.URL, err.StatusCode)
}

// NewClient returns a synchronization client. Invalid options are reported by
// Sync so construction retains the small API shown in the specification.
func NewClient(options ClientOptions) *Client {
	normalized, err := normalizeClientOptions(options)
	client := &Client{options: normalized, configErr: err}
	if err != nil {
		return client
	}

	httpClient := http.Client{}
	if options.HTTPClient != nil {
		httpClient = *options.HTTPClient
	}
	policy := addressPolicy{
		allowPrivate:        normalized.allowPrivateAddresses,
		responseIdleTimeout: normalized.responseIdleTimeout,
	}
	protected, transportErr := protectedTransport(httpClient.Transport, policy)
	if transportErr != nil {
		client.configErr = transportErr
		return client
	}
	httpClient.Transport = protected
	previousRedirect := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= normalized.maxRedirects {
			return fmt.Errorf("%w: stopped after %d redirects", ErrUnsafeURL, normalized.maxRedirects)
		}
		if err := validateRemoteURL(request.URL); err != nil {
			return err
		}
		if previousRedirect != nil {
			return previousRedirect(request, via)
		}
		return nil
	}
	client.http = &httpClient
	return client
}

func normalizeClientOptions(options ClientOptions) (normalizedClientOptions, error) {
	normalized := normalizedClientOptions{
		parserOptions:         options.ParserOptions,
		maxPropertiesBytes:    options.MaxPropertiesBytes,
		maxCompressedBytes:    options.MaxCompressedBytes,
		maxRedirects:          options.MaxRedirects,
		maxRetries:            options.MaxRetries,
		retryBaseDelay:        options.RetryBaseDelay,
		maxRetryDelay:         options.MaxRetryDelay,
		responseIdleTimeout:   options.ResponseIdleTimeout,
		userAgent:             options.UserAgent,
		allowPrivateAddresses: options.AllowPrivateAddresses,
	}
	if normalized.maxPropertiesBytes == 0 {
		normalized.maxPropertiesBytes = DefaultMaxPropertiesBytes
	}
	if normalized.maxCompressedBytes == 0 {
		normalized.maxCompressedBytes = DefaultMaxCompressedBytes
	}
	if normalized.maxRedirects == 0 {
		normalized.maxRedirects = DefaultMaxRedirects
	}
	if normalized.maxRetries == 0 {
		normalized.maxRetries = DefaultMaxRetries
	}
	if normalized.retryBaseDelay == 0 {
		normalized.retryBaseDelay = defaultRetryBaseDelay
	}
	if normalized.maxRetryDelay == 0 {
		normalized.maxRetryDelay = defaultMaxRetryDelay
	}
	if normalized.responseIdleTimeout == 0 {
		normalized.responseIdleTimeout = defaultIdleTimeout
	}
	if normalized.userAgent == "" {
		normalized.userAgent = defaultUserAgent()
	}

	if normalized.maxPropertiesBytes < 0 {
		return normalizedClientOptions{}, errors.New("nexus: maximum properties bytes must not be negative")
	}
	if normalized.maxCompressedBytes < 0 {
		return normalizedClientOptions{}, errors.New("nexus: maximum compressed bytes must not be negative")
	}
	if normalized.maxRedirects < 0 {
		return normalizedClientOptions{}, errors.New("nexus: maximum redirects must not be negative")
	}
	if normalized.maxRetries < 0 {
		return normalizedClientOptions{}, errors.New("nexus: maximum retries must not be negative")
	}
	if normalized.retryBaseDelay < 0 || normalized.maxRetryDelay < 0 {
		return normalizedClientOptions{}, errors.New("nexus: retry delays must not be negative")
	}
	if normalized.responseIdleTimeout < 0 {
		return normalizedClientOptions{}, errors.New("nexus: response idle timeout must not be negative")
	}
	return normalized, nil
}

func defaultUserAgent() string {
	version := Version
	if version == "" || version == "dev" {
		version = "devel"
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
	}
	version = strings.TrimPrefix(version, "v")
	return "git-pkgs-nexus/" + version
}

type checkingRoundTripper struct {
	base                http.RoundTripper
	policy              addressPolicy
	responseIdleTimeout time.Duration
}

func (transport *checkingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := validateRemoteURL(request.URL); err != nil {
		return nil, err
	}
	if err := transport.policy.checkHost(request.Context(), request.URL.Hostname()); err != nil {
		return nil, err
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.Body != nil && transport.responseIdleTimeout > 0 {
		response.Body = newIdleReadCloser(response.Body, transport.responseIdleTimeout)
	}
	return response, nil
}

type addressPolicy struct {
	allowPrivate        bool
	responseIdleTimeout time.Duration
}

func (policy addressPolicy) checkHost(ctx context.Context, host string) error {
	if policy.allowPrivate {
		return nil
	}
	if host == "" {
		return errors.New("nexus: remote URL has no host")
	}
	if parsed := net.ParseIP(host); parsed != nil {
		return checkPublicIP(parsed)
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("nexus: resolve %s: %w", host, err)
	}
	if len(addresses) == 0 {
		return fmt.Errorf("nexus: no addresses resolved for %s", host)
	}
	for _, address := range addresses {
		if err := checkPublicIP(address.IP); err != nil {
			return fmt.Errorf("nexus: host %s: %w", host, err)
		}
	}
	return nil
}

const (
	idleBodyOpen uint32 = iota
	idleBodyClosed
	idleBodyTimedOut
)

type idleReadCloser struct {
	body      io.ReadCloser
	timeout   time.Duration
	state     atomic.Uint32
	activity  chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

func newIdleReadCloser(body io.ReadCloser, timeout time.Duration) *idleReadCloser {
	reader := &idleReadCloser{
		body:     body,
		timeout:  timeout,
		activity: make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
	go reader.monitor()
	return reader
}

func (reader *idleReadCloser) Read(buffer []byte) (int, error) {
	if reader.state.Load() == idleBodyTimedOut {
		return 0, responseIdleTimeoutError{timeout: reader.timeout}
	}
	read, err := reader.body.Read(buffer)
	if reader.state.Load() == idleBodyTimedOut {
		return read, responseIdleTimeoutError{timeout: reader.timeout}
	}
	if read > 0 {
		select {
		case reader.activity <- struct{}{}:
		default:
		}
	}
	if err != nil {
		reader.stopMonitor()
	}
	return read, err
}

func (reader *idleReadCloser) Close() error {
	reader.state.CompareAndSwap(idleBodyOpen, idleBodyClosed)
	reader.stopMonitor()
	reader.closeBody()
	return reader.closeErr
}

func (reader *idleReadCloser) monitor() {
	timer := time.NewTimer(reader.timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if reader.state.CompareAndSwap(idleBodyOpen, idleBodyTimedOut) {
				reader.closeBody()
			}
			return
		case <-reader.activity:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(reader.timeout)
		case <-reader.stop:
			return
		}
	}
}

func (reader *idleReadCloser) stopMonitor() {
	reader.stopOnce.Do(func() {
		close(reader.stop)
	})
}

func (reader *idleReadCloser) closeBody() {
	reader.closeOnce.Do(func() {
		reader.closeErr = reader.body.Close()
	})
}

type responseIdleTimeoutError struct {
	timeout time.Duration
}

func (err responseIdleTimeoutError) Error() string {
	return fmt.Sprintf("%s after %s", ErrResponseIdleTimeout, err.timeout)
}

func (err responseIdleTimeoutError) Unwrap() error {
	return ErrResponseIdleTimeout
}

func (err responseIdleTimeoutError) Timeout() bool {
	return true
}

func (err responseIdleTimeoutError) Temporary() bool {
	return false
}

var (
	carrierGradeNAT = mustNetwork("100.64.0.0/10")
	wellKnownNAT64  = mustNetwork("64:ff9b::/96")
	localUseNAT64   = mustNetwork("64:ff9b:1::/48")
)

func mustNetwork(cidr string) *net.IPNet {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return network
}

func checkPublicIP(address net.IP) error {
	kind := ""
	switch {
	case address.IsUnspecified():
		kind = "unspecified"
	case address.IsLoopback():
		kind = "loopback"
	case address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast():
		kind = "link-local"
	case address.IsMulticast() || address.IsInterfaceLocalMulticast():
		kind = "multicast"
	case address.IsPrivate():
		kind = "private"
	case carrierGradeNAT.Contains(address):
		kind = "CGNAT"
	case wellKnownNAT64.Contains(address) || localUseNAT64.Contains(address):
		kind = "NAT64"
	}
	if kind != "" {
		return fmt.Errorf("%w: %s (%s)", ErrPrivateAddress, address, kind)
	}
	return nil
}

func validateRepositoryURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("nexus: parse repository URL: %w", err)
	}
	if err := validateRemoteURL(parsed); err != nil {
		return nil, err
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: repository URL must not contain a query or fragment", ErrUnsafeURL)
	}
	return parsed, nil
}

func validateRemoteURL(remote *url.URL) error {
	if remote == nil {
		return fmt.Errorf("%w: nil remote URL", ErrUnsafeURL)
	}
	if remote.Scheme != "http" && remote.Scheme != "https" {
		return fmt.Errorf("%w: unsupported remote URL scheme %q", ErrUnsafeURL, remote.Scheme)
	}
	if remote.Host == "" {
		return fmt.Errorf("%w: remote URL has no host", ErrUnsafeURL)
	}
	if remote.User != nil {
		return fmt.Errorf("%w: credentials in remote URLs are not allowed", ErrUnsafeURL)
	}
	return nil
}

func resolveRepositoryPath(repository *url.URL, relative string) *url.URL {
	resolved := repository.JoinPath(relative)
	resolved.RawQuery = ""
	resolved.Fragment = ""
	return resolved
}

func (client *Client) do(ctx context.Context, remote *url.URL, headers http.Header) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, remote.String(), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Accept-Encoding", "identity")
		request.Header.Set("User-Agent", client.options.userAgent)
		for name, values := range headers {
			for _, value := range values {
				request.Header.Add(name, value)
			}
		}

		response, err := client.http.Do(request)
		if err == nil && !retryableStatus(response.StatusCode) {
			return response, nil
		}
		if err != nil && !retryableRequestError(ctx, err) {
			return nil, err
		}
		if attempt >= client.options.maxRetries {
			if err != nil {
				return nil, err
			}
			return response, nil
		}
		if response != nil {
			_ = response.Body.Close()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		delay := client.retryDelay(attempt, response)
		if err := waitContext(ctx, delay); err != nil {
			return nil, err
		}
	}
}

func retryableRequestError(ctx context.Context, err error) bool {
	return ctx.Err() == nil &&
		!errors.Is(err, ErrPrivateAddress) &&
		!errors.Is(err, ErrUnsafeURL)
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

func (client *Client) retryDelay(attempt int, response *http.Response) time.Duration {
	if response != nil {
		if retryAfter, ok := parseRetryAfter(response.Header.Get("Retry-After")); ok {
			return retryAfter
		}
	}
	delay := client.options.retryBaseDelay
	for range attempt {
		if delay >= client.options.maxRetryDelay/2 {
			return client.options.maxRetryDelay
		}
		delay *= 2
	}
	return min(delay, client.options.maxRetryDelay)
}

func parseRetryAfter(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64((time.Duration(1<<63-1))/time.Second) {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(time.Until(when), 0), true
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func closeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

func statusError(response *http.Response) error {
	remote := "unknown URL"
	if response.Request != nil && response.Request.URL != nil {
		remote = response.Request.URL.String()
	}
	return &HTTPStatusError{StatusCode: response.StatusCode, URL: remote}
}

func boundedBody(response *http.Response, limit int64) (io.ReadCloser, error) {
	if response.ContentLength > limit {
		closeResponse(response)
		return nil, fmt.Errorf("%w: content length is %d, maximum is %d", ErrCompressedLimit, response.ContentLength, limit)
	}
	return &maxReadCloser{reader: response.Body, remaining: limit, limit: limit}, nil
}

type maxReadCloser struct {
	reader    io.ReadCloser
	remaining int64
	limit     int64
}

func (reader *maxReadCloser) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.remaining == 0 {
		var probe [1]byte
		read, err := reader.reader.Read(probe[:])
		if read > 0 {
			return 0, fmt.Errorf("%w: maximum is %d bytes", ErrCompressedLimit, reader.limit)
		}
		return 0, err
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:int(reader.remaining)]
	}
	read, err := reader.reader.Read(buffer)
	reader.remaining -= int64(read)
	return read, err
}

func (reader *maxReadCloser) Close() error {
	return reader.reader.Close()
}
