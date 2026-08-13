package nexus

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const negativeCounterError = "must not be negative"

func TestParseProperties(t *testing.T) {
	input := `#Thu Aug 13 04:01:32 UTC 2026
nexus.index.id=central
nexus.index.chain-id=1318453614498
nexus.index.timestamp=20260813030842.666 +0000
nexus.index.incremental-2=933
nexus.index.incremental-0=935
nexus.index.incremental-1=934
nexus.index.last-incremental=935
nexus.index.time=20120615133728.952 +0000
`

	properties, err := ParseProperties(strings.NewReader(input), PropertiesOptions{})
	if err != nil {
		t.Fatalf("ParseProperties: %v", err)
	}
	if properties.ID != "central" {
		t.Errorf("ID = %q, want central", properties.ID)
	}
	if properties.ChainID != "1318453614498" {
		t.Errorf("ChainID = %q", properties.ChainID)
	}
	wantTimestamp := time.Date(2026, time.August, 13, 3, 8, 42, 666000000, time.UTC)
	if !properties.Timestamp.Equal(wantTimestamp) {
		t.Errorf("Timestamp = %v, want %v", properties.Timestamp, wantTimestamp)
	}
	if properties.LastIncremental == nil || *properties.LastIncremental != 935 {
		t.Errorf("LastIncremental = %v, want 935", properties.LastIncremental)
	}
	if want := []int64{933, 934, 935}; !reflect.DeepEqual(properties.Incrementals, want) {
		t.Errorf("Incrementals = %v, want %v", properties.Incrementals, want)
	}
	if got := properties.Unknown["nexus.index.time"]; got != "20120615133728.952 +0000" {
		t.Errorf("unknown property = %q", got)
	}
}

func TestParsePropertiesJavaSyntax(t *testing.T) {
	input := "  # comment\r\n" +
		"nexus\\.index\\.id : escaped\\ value\r\n" +
		"nexus.index.timestamp=20260813030842.666 +0000\n" +
		"continued=first\\\n\t second\n" +
		"unicode=Homebr\\u0065w \\uD83D\\uDE80\n" +
		"escaped\\:key=value\\=part\n"

	properties, err := ParseProperties(strings.NewReader(input), PropertiesOptions{})
	if err != nil {
		t.Fatalf("ParseProperties: %v", err)
	}
	if properties.ID != "escaped value" {
		t.Errorf("ID = %q", properties.ID)
	}
	if got := properties.Unknown["continued"]; got != "firstsecond" {
		t.Errorf("continued = %q", got)
	}
	if got := properties.Unknown["unicode"]; got != "Homebrew 🚀" {
		t.Errorf("unicode = %q", got)
	}
	if got := properties.Unknown["escaped:key"]; got != "value=part" {
		t.Errorf("escaped key = %q", got)
	}
}

func TestParsePropertiesErrors(t *testing.T) {
	validTimestamp := "nexus.index.timestamp=20260813030842.666 +0000\n"
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"missing ID", validTimestamp, "missing required property"},
		{"missing timestamp", "nexus.index.id=central\n", "missing required property"},
		{"invalid timestamp", "nexus.index.id=central\nnexus.index.timestamp=yesterday\n", "cannot parse"},
		{"duplicate key", "nexus.index.id=a\nnexus.index.id=b\n" + validTimestamp, "duplicate key"},
		{"invalid slot", "nexus.index.id=a\n" + validTimestamp + "nexus.index.last-incremental=2\nnexus.index.incremental-x=2\n", "counter"},
		{"negative counter", "nexus.index.id=a\n" + validTimestamp + "nexus.index.last-incremental=-1\n", negativeCounterError},
		{"duplicate counter", "nexus.index.id=a\n" + validTimestamp + "nexus.index.last-incremental=2\nnexus.index.incremental-0=2\nnexus.index.incremental-1=2\n", "duplicate incremental"},
		{"slot without last", "nexus.index.id=a\n" + validTimestamp + "nexus.index.incremental-0=2\n", "require"},
		{"counter ahead", "nexus.index.id=a\n" + validTimestamp + "nexus.index.last-incremental=2\nnexus.index.incremental-0=3\n", "ahead"},
		{"bad Unicode", "nexus.index.id=a\n" + validTimestamp + "value=\\u12xz\n", "invalid Unicode escape"},
		{"unpaired surrogate", "nexus.index.id=a\n" + validTimestamp + "value=\\uD83D\n", "unpaired Unicode surrogate"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseProperties(strings.NewReader(test.input), PropertiesOptions{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestParsePropertiesSizeLimit(t *testing.T) {
	_, err := ParseProperties(strings.NewReader("nexus.index.id=central\n"), PropertiesOptions{MaxBytes: 4})
	if !errors.Is(err, ErrPropertiesTooLarge) {
		t.Fatalf("error = %v, want ErrPropertiesTooLarge", err)
	}
}

func TestParsePropertiesReadError(t *testing.T) {
	_, err := ParseProperties(errorReader{}, PropertiesOptions{})
	if err == nil || !strings.Contains(err.Error(), "read index properties") {
		t.Fatalf("error = %v", err)
	}
}

func FuzzParseProperties(f *testing.F) {
	f.Add([]byte("nexus.index.id=central\nnexus.index.timestamp=20260813030842.666 +0000\n"))
	f.Add([]byte("key=value\\\n continued\n"))
	f.Add([]byte("unicode=\\uD83D\\uDE80\n"))

	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = ParseProperties(bytes.NewReader(input), PropertiesOptions{MaxBytes: 4096})
	})
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("broken reader")
}
