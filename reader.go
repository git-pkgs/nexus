package nexus

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	// DefaultMaxDecompressedBytes permits chunks up to 64 GiB after gzip.
	DefaultMaxDecompressedBytes int64 = 64 << 30
	// DefaultMaxRecords permits up to 250 million records in one chunk.
	DefaultMaxRecords int64 = 250_000_000
	// DefaultMaxFieldsPerRecord permits up to 256 fields in one record.
	DefaultMaxFieldsPerRecord int64 = 256
	// DefaultMaxFieldNameBytes permits the complete unsigned 16-bit name size.
	DefaultMaxFieldNameBytes int64 = 1<<16 - 1
	// DefaultMaxFieldValueBytes permits a field value up to 64 MiB.
	DefaultMaxFieldValueBytes int64 = 64 << 20
	// DefaultMaxRecordBytes bounds encoded name and value data in one record to
	// 128 MiB.
	DefaultMaxRecordBytes int64 = 128 << 20

	supportedChunkVersion = 1
	discardBufferSize     = 32 << 10
	selectedFieldCapacity = 7
)

var (
	// ErrClosed is returned when a read is attempted after Close.
	ErrClosed = errors.New("nexus: reader is closed")
	// ErrDecompressedLimit is returned when a chunk exceeds its byte limit.
	ErrDecompressedLimit = errors.New("nexus: decompressed chunk exceeds size limit")
	// ErrRecordLimit is returned when a chunk contains too many records.
	ErrRecordLimit = errors.New("nexus: chunk exceeds record limit")
	// ErrTrailingData is returned for another gzip member or trailing bytes.
	ErrTrailingData = errors.New("nexus: trailing data after gzip member")
	// ErrTruncated is returned when a binary value ends partway through.
	ErrTruncated = errors.New("nexus: truncated chunk")
	// ErrUnsupportedVersion is returned for an unknown chunk format version.
	ErrUnsupportedVersion = errors.New("nexus: unsupported chunk version")
)

// Options controls chunk parsing. Every zero field uses its default.
type Options struct {
	MaxDecompressedBytes int64
	MaxRecords           int64
	MaxFieldsPerRecord   int64
	MaxFieldNameBytes    int64
	MaxFieldValueBytes   int64
	MaxRecordBytes       int64
}

// Header is the binary header at the start of a chunk.
type Header struct {
	Version   uint8
	Timestamp time.Time
}

// Field is one raw field from an index record.
type Field struct {
	Flags uint8
	Name  string
	Value string
}

// Record retains raw fields in file order, including duplicate names.
type Record struct {
	Fields []Field
}

// Value returns the last value with the supplied name. Maven Indexer's Java
// reader uses the same last-value behavior when mapping a raw record.
func (record Record) Value(name string) (string, bool) {
	for index := len(record.Fields) - 1; index >= 0; index-- {
		if record.Fields[index].Name == name {
			return record.Fields[index].Value, true
		}
	}
	return "", false
}

type normalizedOptions struct {
	maxDecompressedBytes int64
	maxRecords           int64
	maxFieldsPerRecord   int64
	maxFieldNameBytes    int64
	maxFieldValueBytes   int64
	maxRecordBytes       int64
}

// RawReader streams raw records from exactly one gzip member.
type RawReader struct {
	compressed    *bufio.Reader
	gzip          *gzip.Reader
	decoded       *maxBytesReader
	options       normalizedOptions
	header        Header
	records       int64
	scratch       [8]byte
	nameBuffer    []byte
	valueBuffer   []byte
	discardBuffer []byte
	done          bool
	closed        bool
	terminal      error
}

// NewRawReader opens a Maven index chunk and reads its binary header.
func NewRawReader(input io.Reader, options Options) (*RawReader, error) {
	if input == nil {
		return nil, errors.New("nexus: nil chunk reader")
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}

	compressed := bufio.NewReader(input)
	gzipReader, err := gzip.NewReader(compressed)
	if err != nil {
		return nil, fmt.Errorf("nexus: open gzip chunk: %w", err)
	}
	gzipReader.Multistream(false)
	decoded := &maxBytesReader{
		reader:    gzipReader,
		remaining: normalized.maxDecompressedBytes,
		limit:     normalized.maxDecompressedBytes,
	}

	var version [1]byte
	if err := readRequired(decoded, version[:], "chunk version"); err != nil {
		_ = gzipReader.Close()
		return nil, err
	}
	if version[0] != supportedChunkVersion {
		_ = gzipReader.Close()
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version[0])
	}

	var timestampBytes [8]byte
	if err := readRequired(decoded, timestampBytes[:], "chunk timestamp"); err != nil {
		_ = gzipReader.Close()
		return nil, err
	}
	timestamp := int64(binary.BigEndian.Uint64(timestampBytes[:]))
	return &RawReader{
		compressed: compressed,
		gzip:       gzipReader,
		decoded:    decoded,
		options:    normalized,
		header: Header{
			Version:   version[0],
			Timestamp: time.UnixMilli(timestamp).UTC(),
		},
	}, nil
}

// Header returns the chunk header read by NewRawReader.
func (reader *RawReader) Header() Header {
	return reader.header
}

// NextRecord returns the next raw record in file order.
func (reader *RawReader) NextRecord() (Record, error) {
	return reader.nextRecord(nil)
}

type fieldSelector func(string) bool

func (reader *RawReader) nextRecord(selector fieldSelector) (Record, error) {
	if reader.closed {
		return Record{}, ErrClosed
	}
	if reader.terminal != nil {
		return Record{}, reader.terminal
	}
	if reader.done {
		return Record{}, io.EOF
	}

	fieldCount, eof, err := reader.readFieldCount()
	if err != nil {
		return reader.fail(err)
	}
	if eof {
		if finishErr := reader.finish(); finishErr != nil {
			return reader.fail(finishErr)
		}
		reader.done = true
		return Record{}, io.EOF
	}
	if reader.records >= reader.options.maxRecords {
		return reader.fail(fmt.Errorf("%w: maximum is %d", ErrRecordLimit, reader.options.maxRecords))
	}
	if fieldCount < 0 {
		return reader.fail(fmt.Errorf("nexus: record %d has negative field count %d", reader.records+1, fieldCount))
	}
	if int64(fieldCount) > reader.options.maxFieldsPerRecord {
		return reader.fail(fmt.Errorf("nexus: record %d has %d fields, maximum is %d", reader.records+1, fieldCount, reader.options.maxFieldsPerRecord))
	}

	fieldCapacity := int(fieldCount)
	if selector != nil {
		fieldCapacity = min(fieldCapacity, selectedFieldCapacity)
	}
	record := Record{Fields: make([]Field, 0, fieldCapacity)}
	var recordBytes int64
	for fieldIndex := int32(0); fieldIndex < fieldCount; fieldIndex++ {
		field, selected, encodedBytes, fieldErr := reader.readField(
			reader.records+1,
			int64(fieldIndex)+1,
			recordBytes,
			selector,
		)
		if fieldErr != nil {
			return reader.fail(fieldErr)
		}
		recordBytes += encodedBytes
		if selected {
			record.Fields = append(record.Fields, field)
		}
	}
	reader.records++
	return record, nil
}

// Close releases parser-owned gzip state. It does not close the caller's
// input reader.
func (reader *RawReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	return reader.gzip.Close()
}

func (reader *RawReader) readFieldCount() (int32, bool, error) {
	encoded := reader.scratch[:4]
	read, err := readDecodedFull(reader.decoded, encoded)
	if err == nil {
		return int32(binary.BigEndian.Uint32(encoded)), false, nil
	}
	if read == 0 && errors.Is(err, io.EOF) {
		return 0, true, nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, false, fmt.Errorf("%w: record field count", ErrTruncated)
	}
	return 0, false, fmt.Errorf("nexus: read record field count: %w", err)
}

func (reader *RawReader) readField(
	recordNumber, fieldNumber, recordBytes int64,
	selector fieldSelector,
) (Field, bool, int64, error) {
	if err := readFieldRequired(reader.decoded, reader.scratch[:1], recordNumber, fieldNumber, "flags"); err != nil {
		return Field{}, false, 0, err
	}
	flags := reader.scratch[0]

	if err := readFieldRequired(reader.decoded, reader.scratch[:2], recordNumber, fieldNumber, "name length"); err != nil {
		return Field{}, false, 0, err
	}
	nameLength := int64(binary.BigEndian.Uint16(reader.scratch[:2]))
	if nameLength > reader.options.maxFieldNameBytes {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d field %d name has %d encoded bytes, maximum is %d", recordNumber, fieldNumber, nameLength, reader.options.maxFieldNameBytes)
	}
	if nameLength > reader.options.maxRecordBytes-recordBytes {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d exceeds encoded byte limit %d", recordNumber, reader.options.maxRecordBytes)
	}
	nameBytes := resizeBuffer(&reader.nameBuffer, int(nameLength))
	if err := readFieldRequired(reader.decoded, nameBytes, recordNumber, fieldNumber, "name"); err != nil {
		return Field{}, false, 0, err
	}
	name, known := canonicalFieldName(nameBytes)
	if !known {
		decodedName, decodeErr := decodeModifiedUTF8(nameBytes)
		if decodeErr != nil {
			return Field{}, false, 0, fmt.Errorf("nexus: record %d field %d name: %w", recordNumber, fieldNumber, decodeErr)
		}
		name = decodedName
	}

	if err := readFieldRequired(reader.decoded, reader.scratch[:4], recordNumber, fieldNumber, "value length"); err != nil {
		return Field{}, false, 0, err
	}
	valueLength := int64(int32(binary.BigEndian.Uint32(reader.scratch[:4])))
	if valueLength < 0 {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d field %d has negative value length %d", recordNumber, fieldNumber, valueLength)
	}
	if valueLength > reader.options.maxFieldValueBytes {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d field %d value has %d encoded bytes, maximum is %d", recordNumber, fieldNumber, valueLength, reader.options.maxFieldValueBytes)
	}
	encodedBytes := nameLength + valueLength
	if encodedBytes > reader.options.maxRecordBytes-recordBytes {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d exceeds encoded byte limit %d", recordNumber, reader.options.maxRecordBytes)
	}
	if selector != nil && !selector(name) {
		if err := reader.validateDiscardedValue(valueLength, recordNumber, fieldNumber); err != nil {
			return Field{}, false, 0, err
		}
		return Field{}, false, encodedBytes, nil
	}
	valueBytes := resizeBuffer(&reader.valueBuffer, int(valueLength))
	if err := readFieldRequired(reader.decoded, valueBytes, recordNumber, fieldNumber, "value"); err != nil {
		return Field{}, false, 0, err
	}
	value, err := decodeModifiedUTF8(valueBytes)
	if err != nil {
		return Field{}, false, 0, fmt.Errorf("nexus: record %d field %d value: %w", recordNumber, fieldNumber, err)
	}
	return Field{Flags: flags, Name: name, Value: value}, true, encodedBytes, nil
}

func (reader *RawReader) validateDiscardedValue(valueLength, recordNumber, fieldNumber int64) error {
	if valueLength > 0 {
		bufferLength := int(min(valueLength, int64(discardBufferSize)))
		resizeBuffer(&reader.discardBuffer, bufferLength)
	}
	validator := modifiedUTF8Validator{}
	remaining := valueLength
	for remaining > 0 {
		readSize := min(remaining, int64(len(reader.discardBuffer)))
		buffer := reader.discardBuffer[:int(readSize)]
		if err := readFieldRequired(reader.decoded, buffer, recordNumber, fieldNumber, "value"); err != nil {
			return err
		}
		if err := validator.Write(buffer); err != nil {
			return fmt.Errorf("nexus: record %d field %d value: %w", recordNumber, fieldNumber, err)
		}
		remaining -= readSize
	}
	if err := validator.Finish(); err != nil {
		return fmt.Errorf("nexus: record %d field %d value: %w", recordNumber, fieldNumber, err)
	}
	return nil
}

func (reader *RawReader) finish() error {
	_, err := reader.compressed.Peek(1)
	if err == nil {
		return ErrTrailingData
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return fmt.Errorf("nexus: read after gzip member: %w", err)
}

func (reader *RawReader) fail(err error) (Record, error) {
	reader.terminal = err
	return Record{}, err
}

type maxBytesReader struct {
	reader    *gzip.Reader
	remaining int64
	limit     int64
}

func (reader *maxBytesReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.remaining == 0 {
		var probe [1]byte
		read, err := reader.reader.Read(probe[:])
		if read > 0 {
			return 0, fmt.Errorf("%w: maximum is %d bytes", ErrDecompressedLimit, reader.limit)
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

func readRequired(reader *maxBytesReader, buffer []byte, description string) error {
	_, err := readDecodedFull(reader, buffer)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %s", ErrTruncated, description)
	}
	return fmt.Errorf("nexus: read %s: %w", description, err)
}

func readFieldRequired(reader *maxBytesReader, buffer []byte, recordNumber, fieldNumber int64, part string) error {
	_, err := readDecodedFull(reader, buffer)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: record %d field %d %s", ErrTruncated, recordNumber, fieldNumber, part)
	}
	return fmt.Errorf("nexus: read record %d field %d %s: %w", recordNumber, fieldNumber, part, err)
}

func readDecodedFull(reader *maxBytesReader, buffer []byte) (int, error) {
	read := 0
	var err error
	for read < len(buffer) && err == nil {
		var count int
		count, err = reader.Read(buffer[read:])
		read += count
	}
	if read == len(buffer) {
		return read, nil
	}
	if read > 0 && errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return read, err
}

func canonicalFieldName(encoded []byte) (string, bool) {
	if len(encoded) == 1 {
		switch encoded[0] {
		case 'u':
			return fieldUInfo, true
		case 'i':
			return fieldInfo, true
		case 'm':
			return fieldModified, true
		case 'n':
			return fieldName, true
		case 'd':
			return fieldDescription, true
		case '1':
			return fieldSHA1, true
		}
	}
	known := [...]string{
		fieldDeleted,
		"DESCRIPTOR",
		"IDXINFO",
		fieldAllGroups,
		"allGroupsList",
		"rootGroups",
		"rootGroupsList",
	}
	for _, name := range known {
		if bytes.Equal(encoded, []byte(name)) {
			return name, true
		}
	}
	return "", false
}

func resizeBuffer(buffer *[]byte, length int) []byte {
	if cap(*buffer) < length {
		*buffer = make([]byte, length)
	} else {
		*buffer = (*buffer)[:length]
	}
	return *buffer
}

func normalizeOptions(options Options) (normalizedOptions, error) {
	normalized := normalizedOptions{
		maxDecompressedBytes: options.MaxDecompressedBytes,
		maxRecords:           options.MaxRecords,
		maxFieldsPerRecord:   options.MaxFieldsPerRecord,
		maxFieldNameBytes:    options.MaxFieldNameBytes,
		maxFieldValueBytes:   options.MaxFieldValueBytes,
		maxRecordBytes:       options.MaxRecordBytes,
	}
	values := []struct {
		name         string
		value        *int64
		defaultValue int64
	}{
		{"decompressed byte", &normalized.maxDecompressedBytes, DefaultMaxDecompressedBytes},
		{"record", &normalized.maxRecords, DefaultMaxRecords},
		{"field per record", &normalized.maxFieldsPerRecord, DefaultMaxFieldsPerRecord},
		{"field name byte", &normalized.maxFieldNameBytes, DefaultMaxFieldNameBytes},
		{"field value byte", &normalized.maxFieldValueBytes, DefaultMaxFieldValueBytes},
		{"record byte", &normalized.maxRecordBytes, DefaultMaxRecordBytes},
	}
	for _, option := range values {
		if *option.value < 0 {
			return normalizedOptions{}, fmt.Errorf("nexus: maximum %s limit must not be negative", option.name)
		}
		if *option.value == 0 {
			*option.value = option.defaultValue
		}
	}
	return normalized, nil
}
