package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

const benchmarkFixtureEnvironment = "NEXUS_BENCHMARK_CHUNK"

func BenchmarkParseProcess(b *testing.B) {
	fixture, fixtureSize := processBenchmarkFixture(b)
	binary, binarySize := buildBenchmarkBinary(b)
	b.SetBytes(fixtureSize)
	b.ResetTimer()
	for b.Loop() {
		command := exec.CommandContext(context.Background(), binary, parseCommand, fixture)
		command.Stdout = io.Discard
		var stderr bytes.Buffer
		command.Stderr = &stderr
		if err := command.Run(); err != nil {
			b.Fatalf("parse process: %v: %s", err, stderr.String())
		}
	}
	b.ReportMetric(float64(binarySize), "binary-bytes")
}

func BenchmarkIncrementalSyncProcess(b *testing.B) {
	fixture, fixtureSize := processBenchmarkFixture(b)
	chunk, err := os.ReadFile(fixture)
	if err != nil {
		b.Fatal(err)
	}
	properties := "nexus.index.id=benchmark\n" +
		"nexus.index.chain-id=benchmark\n" +
		"nexus.index.timestamp=20260813040906.000 +0000\n" +
		"nexus.index.incremental-0=1\n" +
		"nexus.index.last-incremental=1\n"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if filepath.Ext(request.URL.Path) == ".properties" {
			_, _ = io.WriteString(writer, properties)
			return
		}
		_, _ = writer.Write(chunk)
	}))
	b.Cleanup(server.Close)

	cursorPath := filepath.Join(b.TempDir(), "cursor.json")
	cursor := `{"index_id":"benchmark","chain_id":"benchmark","last_incremental":0,"timestamp":"2026-08-12T04:09:06Z"}`
	if err := os.WriteFile(cursorPath, []byte(cursor), 0o600); err != nil {
		b.Fatal(err)
	}
	binary, binarySize := buildBenchmarkBinary(b)
	b.SetBytes(fixtureSize + int64(len(properties)))
	b.ResetTimer()
	for b.Loop() {
		command := exec.CommandContext(context.Background(), binary, syncCommand, "--allow-private", "--cursor", cursorPath, server.URL)
		command.Stdout = io.Discard
		var stderr bytes.Buffer
		command.Stderr = &stderr
		if err := command.Run(); err != nil {
			b.Fatalf("sync process: %v: %s", err, stderr.String())
		}
	}
	b.ReportMetric(float64(binarySize), "binary-bytes")
}

func processBenchmarkFixture(b *testing.B) (string, int64) {
	b.Helper()
	fixture := os.Getenv(benchmarkFixtureEnvironment)
	if fixture == "" {
		b.Skipf("set %s to a saved Maven index chunk", benchmarkFixtureEnvironment)
	}
	data, err := os.ReadFile(fixture)
	if err != nil {
		b.Fatal(err)
	}
	digest := sha256.Sum256(data)
	b.Logf("fixture sha256=%x; go=%s; os=%s; arch=%s; gomaxprocs=%d", digest, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
	return fixture, int64(len(data))
}

func buildBenchmarkBinary(b *testing.B) (string, int64) {
	b.Helper()
	binary := filepath.Join(b.TempDir(), "nexus")
	command := exec.Command("go", "build", "-trimpath", "-o", binary, ".")
	var output bytes.Buffer
	command.Stderr = &output
	if err := command.Run(); err != nil {
		b.Fatalf("build command: %v: %s", err, output.String())
	}
	info, err := os.Stat(binary)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("binary=%s; size=%s", binary, byteCount(info.Size()))
	return binary, info.Size()
}

func byteCount(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	value := float64(size)
	units := []string{"KiB", "MiB", "GiB"}
	for _, suffix := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f TiB", value/unit)
}

func TestByteCount(t *testing.T) {
	tests := map[int64]string{
		42:              "42 B",
		2 * 1024:        "2.0 KiB",
		3 * 1024 * 1024: "3.0 MiB",
	}
	for size, want := range tests {
		if got := byteCount(size); got != want {
			t.Errorf("byteCount(%d) = %q, want %q", size, got, want)
		}
	}
}
