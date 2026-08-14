package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	commandTimestamp  = "20260813040906.000 +0000"
	commandIndexID    = "releases"
	commandArtifactID = "g|a|1|NA|jar"
)

func TestRunParse(t *testing.T) {
	chunk := commandTestChunk(t)
	tests := []struct {
		name  string
		args  func(*testing.T) []string
		stdin io.Reader
	}{
		{
			name:  "stdin",
			args:  func(*testing.T) []string { return []string{parseCommand, "-"} },
			stdin: bytes.NewReader(chunk),
		},
		{
			name: "file",
			args: func(t *testing.T) []string {
				path := filepath.Join(t.TempDir(), "index.gz")
				if err := os.WriteFile(path, chunk, 0o600); err != nil {
					t.Fatal(err)
				}
				return []string{parseCommand, path}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			if err := run(context.Background(), test.args(t), test.stdin, &stdout, io.Discard); err != nil {
				t.Fatal(err)
			}
			want := "{\"type\":\"add\",\"group_id\":\"g\",\"artifact_id\":\"a\",\"version\":\"1\",\"extension\":\"jar\"}\n"
			if stdout.String() != want {
				t.Errorf("output = %q, want %q", stdout.String(), want)
			}
		})
	}
}

func TestRunSync(t *testing.T) {
	chunk := commandTestChunk(t)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		switch request.URL.Path {
		case "/repo/.index/nexus-maven-repository-index.properties":
			_, _ = io.WriteString(writer, "nexus.index.id="+commandIndexID+"\nnexus.index.timestamp="+commandTimestamp+"\n")
		case "/repo/.index/nexus-maven-repository-index.gz":
			_, _ = writer.Write(chunk)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	err := run(context.Background(), []string{syncCommand, "--allow-private", server.URL + "/repo/"}, strings.NewReader(""), &stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		`{"type":"sync","mode":"full","index_id":"releases"}`,
		`{"type":"add","group_id":"g","artifact_id":"a","version":"1","extension":"jar"}`,
		`{"type":"checkpoint","cursor":{"index_id":"releases","timestamp":"2026-08-13T04:09:06Z"}}`,
		"",
	}, "\n")
	if stdout.String() != want {
		t.Errorf("output = %q, want %q", stdout.String(), want)
	}
	wantPaths := []string{
		"/repo/.index/nexus-maven-repository-index.properties",
		"/repo/.index/nexus-maven-repository-index.gz",
		"/repo/.index/nexus-maven-repository-index.properties",
	}
	if strings.Join(paths, "\n") != strings.Join(wantPaths, "\n") {
		t.Errorf("paths = %v, want %v", paths, wantPaths)
	}
}

func TestRunSyncCurrentFromCursor(t *testing.T) {
	cursorPath := filepath.Join(t.TempDir(), "cursor.json")
	cursor := `{"index_id":"releases","timestamp":"2026-08-13T04:09:06Z","etag":"properties-v1"}`
	if err := os.WriteFile(cursorPath, []byte(cursor), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("If-None-Match") != "properties-v1" {
			t.Errorf("If-None-Match = %q", request.Header.Get("If-None-Match"))
		}
		writer.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()

	var stdout bytes.Buffer
	err := run(context.Background(), []string{syncCommand, "--cursor", cursorPath, "--allow-private", server.URL}, strings.NewReader(""), &stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"type\":\"sync\",\"mode\":\"current\",\"index_id\":\"releases\"}\n"
	if stdout.String() != want {
		t.Errorf("output = %q, want %q", stdout.String(), want)
	}
}

func TestReadCursorStrictJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"unknown field", `{"index_id":"x","extra":true}`, "unknown field"},
		{"trailing value", `{"index_id":"x"} {}`, "trailing JSON"},
		{"malformed", `{`, "decode cursor"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cursor.json")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := readCursor(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestReadCursorSizeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	data := bytes.Repeat([]byte{' '}, int(maxCursorBytes+1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := readCursor(path)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunUsageErrors(t *testing.T) {
	tests := [][]string{
		nil,
		{"unknown"},
		{parseCommand},
		{syncCommand},
	}
	for _, args := range tests {
		if err := run(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard); err == nil {
			t.Errorf("run(%v) returned no error", args)
		}
	}
}

func TestRunParseWriteError(t *testing.T) {
	err := run(context.Background(), []string{parseCommand, "-"}, bytes.NewReader(commandTestChunk(t)), failingWriter{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "write event") {
		t.Fatalf("error = %v", err)
	}
}

func commandTestChunk(t *testing.T) []byte {
	t.Helper()
	var body bytes.Buffer
	body.WriteByte(1)
	writeCommandUint64(&body, uint64(time.Date(2026, time.August, 13, 4, 9, 6, 0, time.UTC).UnixMilli()))
	writeCommandUint32(&body, 1)
	body.WriteByte(0)
	writeCommandUint16(&body, 1)
	body.WriteByte('u')
	writeCommandUint32(&body, uint32(len(commandArtifactID)))
	_, _ = io.WriteString(&body, commandArtifactID)

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func writeCommandUint16(writer io.Writer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeCommandUint32(writer io.Writer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeCommandUint64(writer io.Writer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
