package nexus

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReaderEmitsArtifactEventsInOrder(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 4, 9, 6, 0, time.UTC)
	modified := timestamp.Add(-time.Minute)
	fileModified := timestamp.Add(-time.Hour)
	records := []Record{
		{Fields: []Field{{Name: "DESCRIPTOR", Value: "NexusIndex"}, {Name: "IDXINFO", Value: "1.0|releases"}}},
		{Fields: []Field{
			{Name: fieldUInfo, Value: "org.example|library|1.2.3|NA"},
			{Name: fieldInfo, Value: packagingJAR + "|" + millis(fileModified) + "|1234|1|0|1|" + packagingJAR},
			{Name: fieldModified, Value: millis(modified)},
			{Name: fieldName, Value: "Library"},
			{Name: fieldDescription, Value: "Description"},
			{Name: fieldSHA1, Value: "abc123"},
		}},
		{Fields: []Field{{Name: fieldAllGroups, Value: fieldAllGroups}}},
		{Fields: []Field{
			{Name: fieldUInfo, Value: "org.example|library|1.2.3|sources|" + packagingJAR},
			{Name: fieldInfo, Value: packagingJAR + "|" + millis(fileModified) + "|42|0|0|0|" + packagingJAR},
		}},
		{Fields: []Field{
			{Name: fieldDeleted, Value: "org.example|library|1.2.2|NA|null"},
			{Name: fieldModified, Value: millis(modified)},
		}},
	}
	chunk := makeTestChunk(t, supportedChunkVersion, timestamp, records)

	reader, err := NewReader(bytes.NewReader(chunk), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	if !reader.Header().Timestamp.Equal(timestamp) {
		t.Errorf("Header = %+v", reader.Header())
	}

	var events []Event
	for {
		event, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		events = append(events, event)
	}
	if len(events) != 3 {
		t.Fatalf("events = %+v", events)
	}

	first := events[0]
	if first.Type != Add {
		t.Errorf("first type = %q", first.Type)
	}
	wantFirst := Artifact{
		GroupID:      "org.example",
		ArtifactID:   "library",
		Version:      "1.2.3",
		Extension:    packagingJAR,
		Packaging:    packagingJAR,
		Name:         "Library",
		Description:  "Description",
		SHA1:         "abc123",
		ModifiedAt:   modified,
		FileModified: fileModified,
		FileSize:     1234,
		HasSources:   true,
		HasSignature: true,
	}
	if !reflect.DeepEqual(first.Artifact, wantFirst) {
		t.Errorf("first artifact = %+v, want %+v", first.Artifact, wantFirst)
	}
	if events[1].Artifact.Classifier != "sources" || events[1].Artifact.Extension != packagingJAR {
		t.Errorf("classified artifact = %+v", events[1].Artifact)
	}
	if events[2].Type != Remove || events[2].Artifact.Extension != "" || !events[2].Artifact.ModifiedAt.Equal(modified) {
		t.Errorf("removal = %+v", events[2])
	}
}

func TestMapRecordExtensionFallback(t *testing.T) {
	tests := []struct {
		name      string
		identity  string
		packaging string
		extension string
		want      string
	}{
		{packagingPOM, testMissingClassifierIdentity, packagingPOM, "", packagingPOM},
		{packagingWAR, testMissingClassifierIdentity, packagingWAR, "", packagingWAR},
		{packagingEAR, testMissingClassifierIdentity, packagingEAR, "", packagingEAR},
		{"unclassified jar", testMissingClassifierIdentity, "bundle", "", packagingJAR},
		{"classified", "g|a|1|tests|zip", "zip", "", "zip"},
		{"explicit", testMissingClassifierIdentity, "bundle", "nupkg", "nupkg"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := test.packaging + "|1|2|0|0|0"
			if test.extension != "" {
				info += "|" + test.extension
			}
			event, emitted, err := mapRecord(Record{Fields: []Field{{Name: fieldUInfo, Value: test.identity}, {Name: fieldInfo, Value: info}}})
			if err != nil {
				t.Fatal(err)
			}
			if !emitted || event.Artifact.Extension != test.want {
				t.Errorf("event = %+v, want extension %q", event, test.want)
			}
		})
	}
}

func TestMapRecordUsesLastDuplicateField(t *testing.T) {
	record := Record{Fields: []Field{
		{Name: fieldUInfo, Value: "g|old|1|NA|" + packagingJAR},
		{Name: fieldUInfo, Value: "g|new|1|NA|" + packagingJAR},
	}}
	event, emitted, err := mapRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !emitted || event.Artifact.ArtifactID != "new" {
		t.Errorf("event = %+v", event)
	}
}

func TestMapRecordErrors(t *testing.T) {
	tests := []struct {
		name   string
		record Record
		want   string
	}{
		{"identity parts", Record{Fields: []Field{{Name: fieldUInfo, Value: "g|a|1"}}}, "want 4 or 5"},
		{"empty identity", Record{Fields: []Field{{Name: fieldUInfo, Value: "g||1|NA"}}}, "empty group ID"},
		{"info parts", Record{Fields: []Field{{Name: fieldUInfo, Value: testMissingClassifierIdentity}, {Name: fieldInfo, Value: packagingJAR + "|1"}}}, "artifact info"},
		{"file time", Record{Fields: []Field{{Name: fieldUInfo, Value: testMissingClassifierIdentity}, {Name: fieldInfo, Value: packagingJAR + "|now|2|0|0|0"}}}, "file modification"},
		{"file size", Record{Fields: []Field{{Name: fieldUInfo, Value: testMissingClassifierIdentity}, {Name: fieldInfo, Value: packagingJAR + "|1|large|0|0|0"}}}, "file size"},
		{"flag", Record{Fields: []Field{{Name: fieldUInfo, Value: testMissingClassifierIdentity}, {Name: fieldInfo, Value: packagingJAR + "|1|2|yes|0|0"}}}, "sources flag"},
		{"modified", Record{Fields: []Field{{Name: fieldDeleted, Value: testMissingClassifierIdentity}, {Name: fieldModified, Value: "now"}}}, "record modification"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := mapRecord(test.record)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestReaderMappingErrorIsTerminal(t *testing.T) {
	records := []Record{
		{Fields: []Field{{Name: fieldUInfo, Value: "invalid"}}},
		{Fields: []Field{{Name: fieldUInfo, Value: "g|a|1|NA|" + packagingJAR}}},
	}
	chunk := makeTestChunk(t, supportedChunkVersion, time.UnixMilli(1).UTC(), records)
	reader, err := NewReader(bytes.NewReader(chunk), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	_, firstErr := reader.Next()
	_, secondErr := reader.Next()
	if firstErr == nil || secondErr == nil || firstErr.Error() != secondErr.Error() {
		t.Fatalf("errors = %v, %v", firstErr, secondErr)
	}
}

func millis(value time.Time) string {
	return strconv.FormatInt(value.UnixMilli(), 10)
}
