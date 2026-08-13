// Package nexus reads and synchronizes Maven repository indexes.
package nexus

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

const (
	// DefaultMaxPropertiesBytes limits an index properties response to 1 MiB.
	DefaultMaxPropertiesBytes int64 = 1 << 20

	indexIDKey          = "nexus.index.id"
	indexTimestampKey   = "nexus.index.timestamp"
	indexChainIDKey     = "nexus.index.chain-id"
	lastIncrementalKey  = "nexus.index.last-incremental"
	incrementalKeyStart = "nexus.index.incremental-"
	indexTimestampForm  = "20060102150405.000 -0700"
	unicodeEscapePrefix = 2
)

// ErrPropertiesTooLarge is returned when a properties file exceeds its
// configured byte limit.
var ErrPropertiesTooLarge = errors.New("nexus: index properties exceed size limit")

// PropertiesOptions controls index properties parsing. The zero value uses
// safe defaults.
type PropertiesOptions struct {
	MaxBytes int64
}

// IndexProperties contains the fields used to plan an index synchronization.
// Incrementals contains advertised chunk counters in ascending order.
type IndexProperties struct {
	ID              string
	Timestamp       time.Time
	ChainID         string
	LastIncremental *int64
	Incrementals    []int64
	Unknown         map[string]string
}

// ParseProperties parses a Maven Indexer properties file using Java
// properties syntax.
func ParseProperties(reader io.Reader, options PropertiesOptions) (IndexProperties, error) {
	if reader == nil {
		return IndexProperties{}, errors.New("nexus: nil index properties reader")
	}

	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxPropertiesBytes
	}
	if maxBytes < 0 {
		return IndexProperties{}, errors.New("nexus: index properties byte limit must not be negative")
	}

	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	data, err := io.ReadAll(io.LimitReader(reader, readLimit))
	if err != nil {
		return IndexProperties{}, fmt.Errorf("nexus: read index properties: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return IndexProperties{}, fmt.Errorf("%w: maximum is %d bytes", ErrPropertiesTooLarge, maxBytes)
	}

	values, err := parseJavaProperties(data)
	if err != nil {
		return IndexProperties{}, err
	}
	return decodeIndexProperties(values)
}

type logicalPropertyLine struct {
	number int
	data   []byte
}

func parseJavaProperties(data []byte) (map[string]string, error) {
	lines := joinPropertyLines(data)
	values := make(map[string]string, len(lines))
	for _, line := range lines {
		keyBytes, valueBytes, ok := splitPropertyLine(line.data)
		if !ok {
			continue
		}

		key, err := decodePropertyString(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("nexus: properties line %d key: %w", line.number, err)
		}
		value, err := decodePropertyString(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("nexus: properties line %d value: %w", line.number, err)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("nexus: properties line %d: duplicate key %q", line.number, key)
		}
		values[key] = value
	}
	return values, nil
}

func joinPropertyLines(data []byte) []logicalPropertyLine {
	physical := splitPropertyLines(data)
	logical := make([]logicalPropertyLine, 0, len(physical))
	var joined []byte
	startLine := 0
	continuing := false

	for index, line := range physical {
		lineNumber := index + 1
		if continuing {
			line = trimPropertySpaceLeft(line)
		} else {
			trimmed := trimPropertySpaceLeft(line)
			if len(trimmed) == 0 || trimmed[0] == '#' || trimmed[0] == '!' {
				continue
			}
			startLine = lineNumber
			joined = joined[:0]
		}

		joined = append(joined, line...)
		if hasContinuation(joined) {
			joined = joined[:len(joined)-1]
			continuing = true
			continue
		}

		lineCopy := append([]byte(nil), joined...)
		logical = append(logical, logicalPropertyLine{number: startLine, data: lineCopy})
		continuing = false
	}

	if continuing {
		lineCopy := append([]byte(nil), joined...)
		logical = append(logical, logicalPropertyLine{number: startLine, data: lineCopy})
	}
	return logical
}

func splitPropertyLines(data []byte) [][]byte {
	lines := make([][]byte, 0, bytesLineCount(data))
	for start := 0; start < len(data); {
		end := start
		for end < len(data) && data[end] != '\n' && data[end] != '\r' {
			end++
		}
		lines = append(lines, data[start:end])
		if end == len(data) {
			break
		}
		if data[end] == '\r' && end+1 < len(data) && data[end+1] == '\n' {
			end++
		}
		start = end + 1
	}
	if len(data) == 0 {
		return nil
	}
	return lines
}

func bytesLineCount(data []byte) int {
	count := 1
	for _, value := range data {
		if value == '\n' {
			count++
		}
	}
	return count
}

func trimPropertySpaceLeft(value []byte) []byte {
	index := 0
	for index < len(value) && isPropertySpace(value[index]) {
		index++
	}
	return value[index:]
}

func isPropertySpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\f'
}

func hasContinuation(value []byte) bool {
	backslashes := 0
	for index := len(value) - 1; index >= 0 && value[index] == '\\'; index-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func splitPropertyLine(line []byte) ([]byte, []byte, bool) {
	start := 0
	for start < len(line) && isPropertySpace(line[start]) {
		start++
	}
	if start == len(line) || line[start] == '#' || line[start] == '!' {
		return nil, nil, false
	}

	keyEnd := start
	escaped := false
	separator := byte(0)
	for keyEnd < len(line) {
		value := line[keyEnd]
		if !escaped && (value == '=' || value == ':' || isPropertySpace(value)) {
			separator = value
			break
		}
		if value == '\\' {
			escaped = !escaped
		} else {
			escaped = false
		}
		keyEnd++
	}

	valueStart := keyEnd
	if separator != 0 {
		if isPropertySpace(separator) {
			for valueStart < len(line) && isPropertySpace(line[valueStart]) {
				valueStart++
			}
			if valueStart < len(line) && (line[valueStart] == '=' || line[valueStart] == ':') {
				valueStart++
			}
		} else {
			valueStart++
		}
		for valueStart < len(line) && isPropertySpace(line[valueStart]) {
			valueStart++
		}
	}

	return line[start:keyEnd], line[valueStart:], true
}

func decodePropertyString(value []byte) (string, error) {
	runes := make([]rune, 0, len(value))
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current != '\\' {
			runes = append(runes, rune(current))
			continue
		}

		index++
		if index == len(value) {
			return "", errors.New("trailing escape")
		}
		switch value[index] {
		case 't':
			runes = append(runes, '\t')
		case 'n':
			runes = append(runes, '\n')
		case 'r':
			runes = append(runes, '\r')
		case 'f':
			runes = append(runes, '\f')
		case 'u':
			decoded, consumed, err := decodePropertyUnicode(value[index+1:])
			if err != nil {
				return "", err
			}
			index += consumed
			if utf16.IsSurrogate(decoded) {
				if decoded < 0xD800 || index+6 >= len(value) || value[index+1] != '\\' || value[index+2] != 'u' {
					return "", fmt.Errorf("unpaired Unicode surrogate U+%04X", decoded)
				}
				low, lowConsumed, lowErr := decodePropertyUnicode(value[index+3:])
				if lowErr != nil {
					return "", lowErr
				}
				combined := utf16.DecodeRune(decoded, low)
				if combined == '\uFFFD' {
					return "", fmt.Errorf("unpaired Unicode surrogate U+%04X", decoded)
				}
				runes = append(runes, combined)
				index += lowConsumed + unicodeEscapePrefix
				continue
			}
			runes = append(runes, decoded)
		default:
			runes = append(runes, rune(value[index]))
		}
	}
	return string(runes), nil
}

func decodePropertyUnicode(value []byte) (rune, int, error) {
	const unicodeDigits = 4
	if len(value) < unicodeDigits {
		return 0, 0, errors.New("incomplete Unicode escape")
	}
	decoded, err := strconv.ParseUint(string(value[:unicodeDigits]), 16, 16)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Unicode escape %q", value[:unicodeDigits])
	}
	return rune(decoded), unicodeDigits, nil
}

func decodeIndexProperties(values map[string]string) (IndexProperties, error) {
	properties := IndexProperties{
		ID:      values[indexIDKey],
		ChainID: values[indexChainIDKey],
		Unknown: make(map[string]string),
	}
	if properties.ID == "" {
		return IndexProperties{}, fmt.Errorf("nexus: missing required property %q", indexIDKey)
	}

	timestampValue := values[indexTimestampKey]
	if timestampValue == "" {
		return IndexProperties{}, fmt.Errorf("nexus: missing required property %q", indexTimestampKey)
	}
	timestamp, err := time.Parse(indexTimestampForm, timestampValue)
	if err != nil {
		return IndexProperties{}, fmt.Errorf("nexus: parse %s: %w", indexTimestampKey, err)
	}
	properties.Timestamp = timestamp.UTC()

	if value, exists := values[lastIncrementalKey]; exists {
		counter, counterErr := parseCounter(lastIncrementalKey, value)
		if counterErr != nil {
			return IndexProperties{}, counterErr
		}
		properties.LastIncremental = &counter
	}

	seenCounters := make(map[int64]string)
	for key, value := range values {
		switch key {
		case indexIDKey, indexTimestampKey, indexChainIDKey, lastIncrementalKey:
			continue
		}
		if !strings.HasPrefix(key, incrementalKeyStart) {
			properties.Unknown[key] = value
			continue
		}

		slot := strings.TrimPrefix(key, incrementalKeyStart)
		if _, slotErr := parseCounter(key+" slot", slot); slotErr != nil {
			return IndexProperties{}, slotErr
		}
		counter, counterErr := parseCounter(key, value)
		if counterErr != nil {
			return IndexProperties{}, counterErr
		}
		if previous, duplicate := seenCounters[counter]; duplicate {
			return IndexProperties{}, fmt.Errorf("nexus: properties %q and %q advertise duplicate incremental %d", previous, key, counter)
		}
		seenCounters[counter] = key
		properties.Incrementals = append(properties.Incrementals, counter)
	}

	if len(properties.Incrementals) > 0 && properties.LastIncremental == nil {
		return IndexProperties{}, fmt.Errorf("nexus: incremental slots require %q", lastIncrementalKey)
	}
	if properties.LastIncremental != nil {
		for _, counter := range properties.Incrementals {
			if counter > *properties.LastIncremental {
				return IndexProperties{}, fmt.Errorf("nexus: incremental %d is ahead of %s=%d", counter, lastIncrementalKey, *properties.LastIncremental)
			}
		}
	}
	sort.Slice(properties.Incrementals, func(left, right int) bool {
		return properties.Incrementals[left] < properties.Incrementals[right]
	})
	return properties, nil
}

func parseCounter(name, value string) (int64, error) {
	counter, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("nexus: parse %s counter %q: %w", name, value, err)
	}
	if counter < 0 {
		return 0, fmt.Errorf("nexus: %s counter must not be negative: %d", name, counter)
	}
	return counter, nil
}
