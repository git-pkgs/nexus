package nexus

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

const (
	testArtifactIdentity          = "org.example|library|1.2.3|NA|jar"
	testDuplicateFieldName        = "duplicate"
	testMissingClassifierIdentity = "g|a|1|NA"
	testShortArtifactIdentity     = "g|a|1|NA|jar"
	testModifiedUTF8Value         = "zero\x00rocket🚀"
	testTimestampWord             = "timestamp"
)

func TestRawReader(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 4, 9, 6, 123000000, time.UTC)
	records := []Record{
		{Fields: []Field{
			{Flags: 7, Name: fieldName, Value: testModifiedUTF8Value},
			{Flags: 1, Name: testDuplicateFieldName, Value: "first"},
			{Flags: 2, Name: testDuplicateFieldName, Value: "last"},
		}},
		{Fields: []Field{{Flags: 4, Name: fieldUInfo, Value: testArtifactIdentity}}},
	}
	chunk := makeTestChunk(t, supportedChunkVersion, timestamp, records)
	input := &trackingReader{Reader: bytes.NewReader(chunk)}

	reader, err := NewRawReader(input, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Header().Version != supportedChunkVersion || !reader.Header().Timestamp.Equal(timestamp) {
		t.Errorf("Header = %+v", reader.Header())
	}

	first, err := reader.NextRecord()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Fields) != 3 || first.Fields[0].Flags != 7 || first.Fields[0].Value != testModifiedUTF8Value {
		t.Errorf("first record = %+v", first)
	}
	if value, ok := first.Value(testDuplicateFieldName); !ok || value != "last" {
		t.Errorf("Value(duplicate) = %q, %v", value, ok)
	}

	second, err := reader.NextRecord()
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := second.Value(fieldUInfo); value != testArtifactIdentity {
		t.Errorf("second record = %+v", second)
	}
	if first.Fields[0].Value != testModifiedUTF8Value || first.Fields[1].Name != testDuplicateFieldName {
		t.Errorf("reading the next record changed the first record: %+v", first)
	}
	if _, err := reader.NextRecord(); !errors.Is(err, io.EOF) {
		t.Fatalf("end error = %v, want EOF", err)
	}
	if _, err := reader.NextRecord(); !errors.Is(err, io.EOF) {
		t.Fatalf("repeated end error = %v, want EOF", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if input.closed {
		t.Error("Close closed the caller's input")
	}
	if _, err := reader.NextRecord(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed error = %v, want ErrClosed", err)
	}
}

func TestRawReaderAllowsConfiguredBoundary(t *testing.T) {
	timestamp := time.UnixMilli(1).UTC()
	chunk := makeTestChunk(t, supportedChunkVersion, timestamp, []Record{{}})
	reader, err := NewRawReader(bytes.NewReader(chunk), Options{MaxRecords: 1, MaxFieldsPerRecord: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	if _, err := reader.NextRecord(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.NextRecord(); !errors.Is(err, io.EOF) {
		t.Fatalf("error = %v, want EOF", err)
	}

	headerOnly := makeTestChunk(t, supportedChunkVersion, timestamp, nil)
	reader, err = NewRawReader(bytes.NewReader(headerOnly), Options{MaxDecompressedBytes: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	if _, err := reader.NextRecord(); !errors.Is(err, io.EOF) {
		t.Fatalf("exact byte limit error = %v, want EOF", err)
	}
}

func TestRawReaderLimits(t *testing.T) {
	timestamp := time.UnixMilli(1).UTC()
	tests := []struct {
		name    string
		records []Record
		options Options
		reads   int
		want    string
		is      error
	}{
		{
			name:    "record count",
			records: []Record{{}, {}},
			options: Options{MaxRecords: 1},
			reads:   2,
			is:      ErrRecordLimit,
		},
		{
			name:    "field count",
			records: []Record{{Fields: []Field{{Name: "a"}, {Name: "b"}}}},
			options: Options{MaxFieldsPerRecord: 1},
			reads:   1,
			want:    "maximum is 1",
		},
		{
			name:    "field name bytes",
			records: []Record{{Fields: []Field{{Name: "ab"}}}},
			options: Options{MaxFieldNameBytes: 1},
			reads:   1,
			want:    "name has 2 encoded bytes",
		},
		{
			name:    "field value bytes",
			records: []Record{{Fields: []Field{{Name: "a", Value: "12"}}}},
			options: Options{MaxFieldValueBytes: 1},
			reads:   1,
			want:    "value has 2 encoded bytes",
		},
		{
			name:    "record bytes",
			records: []Record{{Fields: []Field{{Name: "a", Value: "12"}}}},
			options: Options{MaxRecordBytes: 2},
			reads:   1,
			want:    "encoded byte limit 2",
		},
		{
			name:    "decompressed bytes",
			records: []Record{{}},
			options: Options{MaxDecompressedBytes: 9},
			reads:   1,
			is:      ErrDecompressedLimit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunk := makeTestChunk(t, supportedChunkVersion, timestamp, test.records)
			reader, err := NewRawReader(bytes.NewReader(chunk), test.options)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestReader(t, reader)
			for read := 0; read < test.reads; read++ {
				_, err = reader.NextRecord()
			}
			if test.is != nil && !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want %v", err, test.is)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestRawReaderRejectsInvalidLengths(t *testing.T) {
	timestamp := time.UnixMilli(1).UTC()
	tests := []struct {
		name string
		body func(*bytes.Buffer)
		want string
	}{
		{
			name: "negative field count",
			body: func(body *bytes.Buffer) {
				writeInt32(body, -1)
			},
			want: "negative field count",
		},
		{
			name: "negative value length",
			body: func(body *bytes.Buffer) {
				writeInt32(body, 1)
				body.WriteByte(0)
				writeUint16(body, 1)
				body.WriteByte('u')
				writeInt32(body, -1)
			},
			want: "negative value length",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := testChunkHeader(supportedChunkVersion, timestamp)
			test.body(body)
			reader, err := NewRawReader(bytes.NewReader(gzipTestBody(t, body.Bytes())), Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestReader(t, reader)
			_, err = reader.NextRecord()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestRawReaderRejectsTruncation(t *testing.T) {
	timestamp := time.UnixMilli(1).UTC()
	tests := []struct {
		name string
		body []byte
	}{
		{testTimestampWord, []byte{supportedChunkVersion, 0, 0}},
		{"field count", append(testChunkHeader(supportedChunkVersion, timestamp).Bytes(), 0, 0)},
		{"field flags", appendRecordPrefix(testChunkHeader(supportedChunkVersion, timestamp).Bytes(), 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunk := gzipTestBody(t, test.body)
			reader, err := NewRawReader(bytes.NewReader(chunk), Options{})
			if test.name == testTimestampWord {
				if !errors.Is(err, ErrTruncated) {
					t.Fatalf("constructor error = %v, want ErrTruncated", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestReader(t, reader)
			if _, err := reader.NextRecord(); !errors.Is(err, ErrTruncated) {
				t.Fatalf("error = %v, want ErrTruncated", err)
			}
		})
	}
}

func TestRawReaderRejectsGzipErrorsAndTrailingData(t *testing.T) {
	timestamp := time.UnixMilli(1).UTC()
	valid := makeTestChunk(t, supportedChunkVersion, timestamp, nil)

	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-8] ^= 0xFF
	assertRawReadError(t, corrupt, gzip.ErrChecksum)

	truncated := valid[:len(valid)-4]
	assertRawReadError(t, truncated, ErrTruncated)

	trailing := append(append([]byte(nil), valid...), 'x')
	assertRawReadError(t, trailing, ErrTrailingData)

	secondMember := gzipTestBody(t, []byte("another member"))
	multistream := append(append([]byte(nil), valid...), secondMember...)
	assertRawReadError(t, multistream, ErrTrailingData)
}

func TestRawReaderRejectsUnsupportedVersion(t *testing.T) {
	chunk := makeTestChunk(t, 2, time.UnixMilli(1).UTC(), nil)
	_, err := NewRawReader(bytes.NewReader(chunk), Options{})
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestRawReaderRejectsInvalidOptions(t *testing.T) {
	chunk := makeTestChunk(t, supportedChunkVersion, time.UnixMilli(1).UTC(), nil)
	_, err := NewRawReader(bytes.NewReader(chunk), Options{MaxRecords: -1})
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("error = %v", err)
	}
}

func FuzzRawReader(f *testing.F) {
	seed, err := makeChunk(supportedChunkVersion, time.UnixMilli(1).UTC(), []Record{{Fields: []Field{{Name: "u", Value: testShortArtifactIdentity}}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("not gzip"))

	f.Fuzz(func(t *testing.T, input []byte) {
		reader, err := NewRawReader(bytes.NewReader(input), Options{
			MaxDecompressedBytes: 1024,
			MaxRecords:           4,
			MaxFieldsPerRecord:   4,
			MaxFieldNameBytes:    64,
			MaxFieldValueBytes:   256,
			MaxRecordBytes:       512,
		})
		if err != nil {
			return
		}
		defer func() {
			_ = reader.Close()
		}()
		for {
			_, err = reader.NextRecord()
			if err != nil {
				return
			}
		}
	})
}

type trackingReader struct {
	*bytes.Reader
	closed bool
}

func (reader *trackingReader) Close() error {
	reader.closed = true
	return nil
}

func makeTestChunk(t testing.TB, version byte, timestamp time.Time, records []Record) []byte {
	t.Helper()
	chunk, err := makeChunk(version, timestamp, records)
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

func makeChunk(version byte, timestamp time.Time, records []Record) ([]byte, error) {
	body := testChunkHeader(version, timestamp)
	for _, record := range records {
		writeInt32(body, int32(len(record.Fields)))
		for _, field := range record.Fields {
			body.WriteByte(field.Flags)
			name := encodeModifiedUTF8(field.Name)
			if len(name) > math.MaxUint16 {
				return nil, errors.New("test field name is too long")
			}
			writeUint16(body, uint16(len(name)))
			body.Write(name)
			value := encodeModifiedUTF8(field.Value)
			if len(value) > math.MaxInt32 {
				return nil, errors.New("test field value is too long")
			}
			writeInt32(body, int32(len(value)))
			body.Write(value)
		}
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body.Bytes()); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func gzipTestBody(t testing.TB, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func testChunkHeader(version byte, timestamp time.Time) *bytes.Buffer {
	body := bytes.NewBuffer(make([]byte, 0, 64))
	body.WriteByte(version)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(timestamp.UnixMilli()))
	body.Write(encoded[:])
	return body
}

func appendRecordPrefix(body []byte, fieldCount int32) []byte {
	buffer := bytes.NewBuffer(append([]byte(nil), body...))
	writeInt32(buffer, fieldCount)
	return buffer.Bytes()
}

func writeInt32(writer io.Writer, value int32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	_, _ = writer.Write(encoded[:])
}

func writeUint16(writer io.Writer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func encodeModifiedUTF8(value string) []byte {
	encoded := make([]byte, 0, len(value))
	for _, current := range value {
		if current > 0xFFFF {
			high, low := utf16.EncodeRune(current)
			encoded = appendModifiedUTF8Unit(encoded, uint16(high))
			encoded = appendModifiedUTF8Unit(encoded, uint16(low))
			continue
		}
		encoded = appendModifiedUTF8Unit(encoded, uint16(current))
	}
	return encoded
}

func appendModifiedUTF8Unit(encoded []byte, unit uint16) []byte {
	switch {
	case unit >= 0x0001 && unit <= 0x007F:
		return append(encoded, byte(unit))
	case unit <= 0x07FF:
		return append(encoded, byte(0xC0|unit>>6), byte(0x80|unit&0x3F))
	default:
		return append(encoded, byte(0xE0|unit>>12), byte(0x80|unit>>6&0x3F), byte(0x80|unit&0x3F))
	}
}

func assertRawReadError(t testing.TB, chunk []byte, target error) {
	t.Helper()
	reader, err := NewRawReader(bytes.NewReader(chunk), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	_, err = reader.NextRecord()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func closeTestReader(t testing.TB, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
