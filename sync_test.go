package nexus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testRepositoryPrefix = "/repository/releases/"
	testRemoteIndexID    = "releases"
	testPropertiesETagV1 = `"properties-v1"`
)

func TestClientColdSync(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 4, 9, 6, 123000000, time.UTC)
	chunkData := makeTestChunk(t, supportedChunkVersion, timestamp, []Record{{Fields: []Field{
		{Name: fieldUInfo, Value: "org.example|library|1.2.3|NA|jar"},
		{Name: fieldInfo, Value: "jar|1|42|0|0|0|jar"},
	}}})
	properties := testIndexProperties(timestamp, testChainID, 42, 41, 42)

	var requestMu stdsync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestMu.Lock()
		requests = append(requests, request.URL.Path)
		requestMu.Unlock()
		if request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("Accept-Encoding = %q", request.Header.Get("Accept-Encoding"))
		}
		if !strings.HasPrefix(request.Header.Get("User-Agent"), "git-pkgs-nexus/") {
			t.Errorf("User-Agent = %q", request.Header.Get("User-Agent"))
		}
		switch request.URL.Path {
		case testRepositoryPrefix + propertiesPath:
			writer.Header().Set("ETag", testPropertiesETagV1)
			writer.Header().Set("Last-Modified", "Thu, 13 Aug 2026 04:09:06 GMT")
			_, _ = io.WriteString(writer, properties)
		case testRepositoryPrefix + fullChunkPath:
			_, _ = writer.Write(chunkData)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newLocalClient()
	synchronization, err := client.Sync(context.Background(), server.URL+testRepositoryPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	if synchronization.Mode() != SyncFull {
		t.Fatalf("Mode = %q, want %q", synchronization.Mode(), SyncFull)
	}

	chunk, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Ref() != (ChunkRef{Path: fullChunkPath}) {
		t.Errorf("Ref = %+v", chunk.Ref())
	}
	if _, err := chunk.Checkpoint(); !errors.Is(err, ErrCheckpointUnavailable) {
		t.Fatalf("early Checkpoint error = %v", err)
	}

	events := consumeTestChunk(t, chunk)
	if len(events) != 1 || events[0].Artifact.ArtifactID != "library" {
		t.Fatalf("events = %+v", events)
	}
	checkpoint, err := chunk.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.IndexID != testRemoteIndexID || checkpoint.ChainID != testChainID || checkpoint.LastIncremental == nil || *checkpoint.LastIncremental != 42 || checkpoint.ETag != testPropertiesETagV1 {
		t.Errorf("Checkpoint = %+v", checkpoint)
	}
	if _, err := synchronization.NextChunk(); !errors.Is(err, io.EOF) {
		t.Fatalf("final NextChunk error = %v", err)
	}

	requestMu.Lock()
	gotRequests := append([]string(nil), requests...)
	requestMu.Unlock()
	wantRequests := []string{
		testRepositoryPrefix + propertiesPath,
		testRepositoryPrefix + fullChunkPath,
	}
	if !reflect.DeepEqual(gotRequests, wantRequests) {
		t.Errorf("requests = %v, want %v", gotRequests, wantRequests)
	}
}

func TestClientConditionalPropertiesCurrent(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 4, 9, 6, 0, time.UTC)
	counter := int64(42)
	cursor := Cursor{
		IndexID:         testRemoteIndexID,
		ChainID:         testChainID,
		LastIncremental: &counter,
		Timestamp:       timestamp,
		ETag:            testPropertiesETagV1,
		LastModified:    "Thu, 13 Aug 2026 04:09:06 GMT",
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != testRepositoryPrefix+propertiesPath {
			t.Errorf("unexpected request %s", request.URL.Path)
		}
		if request.Header.Get("If-None-Match") != cursor.ETag {
			t.Errorf("If-None-Match = %q", request.Header.Get("If-None-Match"))
		}
		if request.Header.Get("If-Modified-Since") != cursor.LastModified {
			t.Errorf("If-Modified-Since = %q", request.Header.Get("If-Modified-Since"))
		}
		writer.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()

	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	if synchronization.Mode() != SyncCurrent {
		t.Errorf("Mode = %q", synchronization.Mode())
	}
	if _, err := synchronization.NextChunk(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextChunk error = %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("requests = %d, want 1", calls.Load())
	}
}

func TestClientIncrementalCheckpoints(t *testing.T) {
	remoteTimestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	firstTimestamp := remoteTimestamp.Add(-time.Minute)
	secondTimestamp := remoteTimestamp
	chunks := map[string][]byte{
		testRepositoryPrefix + ".index/nexus-maven-repository-index.41.gz": makeTestChunk(t, supportedChunkVersion, firstTimestamp, []Record{{Fields: []Field{{Name: fieldDeleted, Value: "g|old|1|NA|jar"}}}}),
		testRepositoryPrefix + ".index/nexus-maven-repository-index.42.gz": makeTestChunk(t, supportedChunkVersion, secondTimestamp, []Record{{Fields: []Field{{Name: fieldUInfo, Value: "g|new|2|NA|jar"}}}}),
	}
	properties := testIndexProperties(remoteTimestamp, testChainID, 42, 41, 42)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == testRepositoryPrefix+propertiesPath {
			writer.Header().Set("ETag", `"properties-v2"`)
			writer.Header().Set("Last-Modified", "Thu, 13 Aug 2026 05:00:00 GMT")
			_, _ = io.WriteString(writer, properties)
			return
		}
		data, ok := chunks[request.URL.Path]
		if !ok {
			http.NotFound(writer, request)
			return
		}
		_, _ = writer.Write(data)
	}))
	defer server.Close()

	last := int64(40)
	cursor := Cursor{IndexID: testRemoteIndexID, ChainID: testChainID, LastIncremental: &last, Timestamp: remoteTimestamp.Add(-time.Hour), ETag: testPropertiesETagV1}
	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	if synchronization.Mode() != SyncIncremental {
		t.Fatalf("Mode = %q", synchronization.Mode())
	}

	first, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := synchronization.NextChunk(); !errors.Is(err, ErrChunkActive) {
		t.Fatalf("concurrent NextChunk error = %v", err)
	}
	consumeTestChunk(t, first)
	firstCheckpoint, err := first.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if firstCheckpoint.LastIncremental == nil || *firstCheckpoint.LastIncremental != 41 || !firstCheckpoint.Timestamp.Equal(firstTimestamp) {
		t.Errorf("first checkpoint = %+v", firstCheckpoint)
	}
	if firstCheckpoint.ETag != "" || firstCheckpoint.LastModified != "" {
		t.Errorf("intermediate checkpoint has future validators: %+v", firstCheckpoint)
	}

	second, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	consumeTestChunk(t, second)
	secondCheckpoint, err := second.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if secondCheckpoint.LastIncremental == nil || *secondCheckpoint.LastIncremental != 42 || !secondCheckpoint.Timestamp.Equal(remoteTimestamp) {
		t.Errorf("second checkpoint = %+v", secondCheckpoint)
	}
	if secondCheckpoint.ETag != `"properties-v2"` || secondCheckpoint.LastModified == "" {
		t.Errorf("final checkpoint validators = %+v", secondCheckpoint)
	}
}

func TestClientRefreshesPropertiesOnceAfterMissingChunk(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	properties := testIndexProperties(timestamp, testChainID, 41, 41)
	chunkData := makeTestChunk(t, supportedChunkVersion, timestamp, []Record{{Fields: []Field{{Name: fieldUInfo, Value: testShortArtifactIdentity}}}})
	var propertiesCalls atomic.Int32
	var chunkCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case testRepositoryPrefix + propertiesPath:
			propertiesCalls.Add(1)
			writer.Header().Set("ETag", `"refreshed"`)
			_, _ = io.WriteString(writer, properties)
		case testRepositoryPrefix + ".index/nexus-maven-repository-index.41.gz":
			if chunkCalls.Add(1) == 1 {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(chunkData)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	last := int64(40)
	cursor := Cursor{IndexID: testRemoteIndexID, ChainID: testChainID, LastIncremental: &last, Timestamp: timestamp.Add(-time.Minute)}
	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	chunk, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	consumeTestChunk(t, chunk)
	checkpoint, err := chunk.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.ETag != `"refreshed"` {
		t.Errorf("Checkpoint = %+v", checkpoint)
	}
	if propertiesCalls.Load() != 2 || chunkCalls.Load() != 2 {
		t.Errorf("properties calls = %d, chunk calls = %d", propertiesCalls.Load(), chunkCalls.Load())
	}
}

func TestClientStopsWhenRefreshedPlanChanges(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		refreshed  string
		wantTarget int64
	}{
		{
			name:       "mode",
			refreshed:  testIndexProperties(timestamp.Add(time.Minute), "replacement-chain", 1, 1),
			wantTarget: 41,
		},
		{
			name:       "target",
			refreshed:  testIndexProperties(timestamp.Add(time.Minute), testChainID, 42, 41, 42),
			wantTarget: 41,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initial := testIndexProperties(timestamp, testChainID, 41, 41)
			var propertiesCalls atomic.Int32
			var fullChunkCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case testRepositoryPrefix + propertiesPath:
					if propertiesCalls.Add(1) == 1 {
						_, _ = io.WriteString(writer, initial)
					} else {
						_, _ = io.WriteString(writer, test.refreshed)
					}
				case testRepositoryPrefix + ".index/nexus-maven-repository-index.41.gz":
					http.NotFound(writer, request)
				case testRepositoryPrefix + fullChunkPath:
					fullChunkCalls.Add(1)
					http.Error(writer, "unexpected full chunk", http.StatusInternalServerError)
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()

			last := int64(40)
			cursor := Cursor{IndexID: testRemoteIndexID, ChainID: testChainID, LastIncremental: &last, Timestamp: timestamp.Add(-time.Minute)}
			synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, &cursor)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestSync(t, synchronization)

			_, err = synchronization.NextChunk()
			if !errors.Is(err, ErrSyncPlanChanged) {
				t.Fatalf("NextChunk error = %v, want ErrSyncPlanChanged", err)
			}
			if synchronization.Mode() != SyncIncremental {
				t.Errorf("Mode = %q, want %q", synchronization.Mode(), SyncIncremental)
			}
			target := synchronization.Target()
			if target.LastIncremental == nil || *target.LastIncremental != test.wantTarget {
				t.Errorf("Target = %+v", target)
			}
			if fullChunkCalls.Load() != 0 {
				t.Errorf("full chunk calls = %d, want 0", fullChunkCalls.Load())
			}
			if _, err := synchronization.NextChunk(); !errors.Is(err, ErrSyncPlanChanged) {
				t.Fatalf("terminal NextChunk error = %v", err)
			}
		})
	}
}

func TestClientDoesNotRefreshMissingChunkRepeatedly(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	properties := testIndexProperties(timestamp, testChainID, 41, 41)
	var propertiesCalls atomic.Int32
	var chunkCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == testRepositoryPrefix+propertiesPath {
			propertiesCalls.Add(1)
			_, _ = io.WriteString(writer, properties)
			return
		}
		chunkCalls.Add(1)
		http.NotFound(writer, request)
	}))
	defer server.Close()

	last := int64(40)
	cursor := Cursor{IndexID: testRemoteIndexID, ChainID: testChainID, LastIncremental: &last, Timestamp: timestamp.Add(-time.Minute)}
	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	_, err = synchronization.NextChunk()
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("NextChunk error = %v", err)
	}
	if propertiesCalls.Load() != 2 || chunkCalls.Load() != 2 {
		t.Errorf("properties calls = %d, chunk calls = %d", propertiesCalls.Load(), chunkCalls.Load())
	}
}

func TestClientClosingChunkEarlyStopsSynchronization(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	chunkData := makeTestChunk(t, supportedChunkVersion, timestamp, []Record{{Fields: []Field{{Name: fieldUInfo, Value: testShortArtifactIdentity}}}})
	server := testIndexServer(t, testIndexProperties(timestamp, "", 0), chunkData)
	defer server.Close()

	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	chunk, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if err := chunk.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Checkpoint(); !errors.Is(err, ErrCheckpointUnavailable) {
		t.Fatalf("Checkpoint error = %v", err)
	}
	if _, err := synchronization.NextChunk(); !errors.Is(err, ErrChunkIncomplete) {
		t.Fatalf("NextChunk error = %v", err)
	}
}

func TestClientDoesNotRestartTruncatedChunk(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	chunkData := makeTestChunk(t, supportedChunkVersion, timestamp, []Record{{Fields: []Field{{Name: fieldUInfo, Value: testShortArtifactIdentity}}}})
	chunkData = chunkData[:len(chunkData)-1]
	properties := testIndexProperties(timestamp, "", 0)
	var chunkCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == testRepositoryPrefix+propertiesPath {
			_, _ = io.WriteString(writer, properties)
			return
		}
		chunkCalls.Add(1)
		_, _ = writer.Write(chunkData)
	}))
	defer server.Close()

	synchronization, err := newLocalClient().Sync(context.Background(), server.URL+testRepositoryPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	chunk, err := synchronization.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chunk.Next(); err != nil {
		t.Fatalf("first event error = %v", err)
	}
	if _, err := chunk.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("truncated EOF error = %v", err)
	}
	if _, err := chunk.Checkpoint(); !errors.Is(err, ErrCheckpointUnavailable) {
		t.Fatalf("Checkpoint error = %v", err)
	}
	if chunkCalls.Load() != 1 {
		t.Errorf("chunk requests = %d, want 1", chunkCalls.Load())
	}
}

func TestClientBlocksPrivateAddressesByDefault(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{MaxRetries: 1, RetryBaseDelay: time.Nanosecond, MaxRetryDelay: time.Nanosecond})
	_, err := client.Sync(context.Background(), server.URL, nil)
	if !errors.Is(err, ErrPrivateAddress) {
		t.Fatalf("Sync error = %v, want ErrPrivateAddress", err)
	}
	if calls.Load() != 0 {
		t.Errorf("server calls = %d, want 0", calls.Load())
	}
}

func TestClientBoundsRetries(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	properties := testIndexProperties(timestamp, "", 0)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(writer, properties)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		AllowPrivateAddresses: true,
		MaxRetries:            2,
		RetryBaseDelay:        time.Nanosecond,
		MaxRetryDelay:         time.Nanosecond,
	})
	synchronization, err := client.Sync(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	if calls.Load() != 3 {
		t.Errorf("requests = %d, want 3", calls.Load())
	}
}

func TestClientResponseLimits(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 5, 0, 0, 0, time.UTC)
	properties := testIndexProperties(timestamp, "", 0)
	chunkData := makeTestChunk(t, supportedChunkVersion, timestamp, nil)
	server := testIndexServer(t, properties, chunkData)
	defer server.Close()

	propertiesClient := NewClient(ClientOptions{AllowPrivateAddresses: true, MaxPropertiesBytes: 8})
	if _, err := propertiesClient.Sync(context.Background(), server.URL+testRepositoryPrefix, nil); !errors.Is(err, ErrPropertiesTooLarge) {
		t.Fatalf("properties error = %v", err)
	}

	chunkClient := NewClient(ClientOptions{AllowPrivateAddresses: true, MaxCompressedBytes: int64(len(chunkData) - 1)})
	synchronization, err := chunkClient.Sync(context.Background(), server.URL+testRepositoryPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestSync(t, synchronization)
	if _, err := synchronization.NextChunk(); !errors.Is(err, ErrCompressedLimit) {
		t.Fatalf("chunk error = %v", err)
	}
}

func newLocalClient() *Client {
	return NewClient(ClientOptions{AllowPrivateAddresses: true})
}

func testIndexProperties(timestamp time.Time, chain string, last int64, incrementals ...int64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s=%s\n", indexIDKey, testRemoteIndexID)
	fmt.Fprintf(&builder, "%s=%s\n", indexTimestampKey, timestamp.Format(indexTimestampForm))
	if chain != "" {
		fmt.Fprintf(&builder, "%s=%s\n", indexChainIDKey, chain)
		fmt.Fprintf(&builder, "%s=%d\n", lastIncrementalKey, last)
	}
	for slot, counter := range incrementals {
		fmt.Fprintf(&builder, "%s%d=%d\n", incrementalKeyStart, slot, counter)
	}
	return builder.String()
}

func testIndexServer(t *testing.T, properties string, chunk []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case testRepositoryPrefix + propertiesPath:
			_, _ = io.WriteString(writer, properties)
		case testRepositoryPrefix + fullChunkPath:
			writer.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			_, _ = writer.Write(chunk)
		default:
			http.NotFound(writer, request)
		}
	}))
}

func consumeTestChunk(t testing.TB, chunk *Chunk) []Event {
	t.Helper()
	var events []Event
	for {
		event, err := chunk.Next()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func closeTestSync(t testing.TB, synchronization *Sync) {
	t.Helper()
	if err := synchronization.Close(); err != nil {
		t.Error(err)
	}
}
