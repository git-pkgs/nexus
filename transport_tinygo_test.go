//go:build tinygo

package nexus

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

//go:embed testdata/maven-index-exporter/nexus-maven-repository-index.gz
var tinyGoIndex []byte

func TestTinyGoClientRequiresAddressOptOut(t *testing.T) {
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("transport called without address-policy opt-out")
		return nil, errors.New("unexpected request")
	})
	original := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = original })

	for _, httpClient := range []*http.Client{nil, {}, {Transport: transport}, {Transport: &http.Transport{}}} {
		client := NewClient(ClientOptions{HTTPClient: httpClient})
		_, err := client.Sync(context.Background(), "https://repo.example.test", nil)
		if !errors.Is(err, ErrUnprotectedTransport) {
			t.Fatalf("Sync error = %v, want ErrUnprotectedTransport", err)
		}
	}
}

func TestTinyGoClientRejectsUnsafeURLs(t *testing.T) {
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("transport called for unsafe URL")
		return nil, errors.New("unexpected request")
	})
	client := NewClient(ClientOptions{
		HTTPClient:            &http.Client{Transport: transport},
		AllowPrivateAddresses: true,
	})
	for _, repository := range []string{
		"ftp://repo.example.test", "https://user:password@repo.example.test", "https://repo.example.test?query=value",
	} {
		_, err := client.Sync(context.Background(), repository, nil)
		if !errors.Is(err, ErrUnsafeURL) {
			t.Fatalf("Sync(%q) error = %v, want ErrUnsafeURL", repository, err)
		}
	}
}

func TestTinyGoClientSync(t *testing.T) {
	for _, useDefault := range []bool{false, true} {
		t.Run(map[bool]string{false: "custom transport", true: "default transport"}[useDefault], func(t *testing.T) {
			testTinyGoSync(t, useDefault)
		})
	}
}

func testTinyGoSync(t *testing.T, useDefault bool) {
	t.Helper()
	timestamp := time.UnixMilli(1635883823455).UTC()
	properties := "nexus.index.id=test\nnexus.index.timestamp=" + timestamp.Format(indexTimestampForm) + "\n"
	var requests []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.URL.Path)
		var body io.Reader
		switch request.URL.Path {
		case "/repository/" + propertiesPath:
			body = strings.NewReader(properties)
		case "/repository/" + fullChunkPath:
			body = bytes.NewReader(tinyGoIndex)
		default:
			t.Fatalf("unexpected request: %s", request.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body), Header: make(http.Header)}, nil
	})
	options := ClientOptions{AllowPrivateAddresses: true}
	if useDefault {
		original := http.DefaultTransport
		http.DefaultTransport = transport
		t.Cleanup(func() { http.DefaultTransport = original })
	} else {
		options.HTTPClient = &http.Client{Transport: transport}
	}
	sync, err := NewClient(options).Sync(context.Background(), "https://repo.example.test/repository", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sync.Close(); err != nil {
			t.Error(err)
		}
	})
	if sync.Mode() != SyncFull {
		t.Fatalf("Mode = %s, want full", sync.Mode())
	}
	chunk, err := sync.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for {
		event, err := chunk.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if len(events) != 7 {
		t.Fatalf("event count = %d, want 7", len(events))
	}
	first := events[0].Artifact
	if first.GroupID != "al.aldi" || first.ArtifactID != "sprova4j" || first.Version != "0.1.0" {
		t.Errorf("first artifact = %+v", first)
	}
	cursor, err := chunk.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if cursor.IndexID != "test" || !cursor.Timestamp.Equal(timestamp) {
		t.Errorf("checkpoint = %+v", cursor)
	}
	if _, err := sync.NextChunk(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextChunk error = %v, want EOF", err)
	}
	if len(requests) != 3 || requests[2] != "/repository/"+propertiesPath {
		t.Errorf("requests = %v, want properties, index, properties", requests)
	}
}
