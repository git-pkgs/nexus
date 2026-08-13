package nexus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const benchmarkRecordCount = 100

const benchmarkProperties = `# benchmark fixture
nexus.index.id=central
nexus.index.chain-id=1318453614498
nexus.index.timestamp=20260813030842.666 +0000
nexus.index.incremental-5=930
nexus.index.incremental-4=931
nexus.index.incremental-3=932
nexus.index.incremental-2=933
nexus.index.incremental-1=934
nexus.index.incremental-0=935
nexus.index.last-incremental=935
`

func BenchmarkParseProperties(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkProperties)))
	for b.Loop() {
		_, err := ParseProperties(strings.NewReader(benchmarkProperties), PropertiesOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildPlan(b *testing.B) {
	remote := testProperties("central", "1318453614498", 935, []int64{930, 931, 932, 933, 934, 935})
	cursor := testCursor("central", "1318453614498", 929, time.Date(2026, time.August, 12, 3, 8, 42, 0, time.UTC))
	b.ReportAllocs()
	for b.Loop() {
		_, err := BuildPlan(remote, &cursor)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeModifiedUTF8(b *testing.B) {
	encoded := encodeModifiedUTF8("org.example|library|1.2.3|sources|jar\x00🚀")
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		_, err := decodeModifiedUTF8(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawReader(b *testing.B) {
	chunk := benchmarkChunk(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	for b.Loop() {
		reader, err := NewRawReader(bytes.NewReader(chunk), Options{})
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err = reader.NextRecord()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(benchmarkRecordCount*b.N)/b.Elapsed().Seconds(), "records/s")
}

func BenchmarkReader(b *testing.B) {
	chunk := benchmarkChunk(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	for b.Loop() {
		reader, err := NewReader(bytes.NewReader(chunk), Options{})
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err = reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(benchmarkRecordCount*b.N)/b.Elapsed().Seconds(), "records/s")
}

func BenchmarkIncrementalSync(b *testing.B) {
	chunkData := benchmarkChunk(b)
	properties := benchmarkProperties
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, ".properties") {
			_, _ = io.WriteString(writer, properties)
			return
		}
		_, _ = writer.Write(chunkData)
	}))
	b.Cleanup(server.Close)

	client := newLocalClient()
	cursor := testCursor("central", "1318453614498", 929, time.Date(2026, time.August, 12, 3, 8, 42, 0, time.UTC))
	const chunkCount = 6
	b.ReportAllocs()
	b.SetBytes(int64(len(properties) + chunkCount*len(chunkData)))
	b.ResetTimer()
	for b.Loop() {
		synchronization, err := client.Sync(context.Background(), server.URL, &cursor)
		if err != nil {
			b.Fatal(err)
		}
		for {
			chunk, nextErr := synchronization.NextChunk()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				b.Fatal(nextErr)
			}
			consumeTestChunk(b, chunk)
		}
		if err := synchronization.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(benchmarkRecordCount*chunkCount*b.N)/b.Elapsed().Seconds(), "records/s")
}

func benchmarkChunk(b testing.TB) []byte {
	b.Helper()
	records := make([]Record, benchmarkRecordCount)
	for index := range records {
		records[index] = Record{Fields: []Field{
			{Name: fieldUInfo, Value: testArtifactIdentity},
			{Name: fieldInfo, Value: packagingJAR + "|1786590546000|1234|1|0|1|" + packagingJAR},
			{Name: fieldModified, Value: "1786590546000"},
			{Name: fieldName, Value: "Example library"},
			{Name: fieldDescription, Value: "A small benchmark record"},
			{Name: fieldSHA1, Value: "0123456789abcdef0123456789abcdef01234567"},
		}}
	}
	return makeTestChunk(b, supportedChunkVersion, time.UnixMilli(1786590546000).UTC(), records)
}
