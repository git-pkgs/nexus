package nexus

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const mavenExporterFixtureDirectory = "testdata/maven-index-exporter"

func TestMavenIndexerCompatibilityFixtures(t *testing.T) {
	sprovaDescription := "Java client for Sprova Test Management"
	versionOneSources := fixtureAddition(
		"al.aldi", "sprova4j", "0.1.1", "sources", "jar", "jar", "sprova4j", sprovaDescription,
		1635883814461, 1626111425534, 14510, false,
	)
	versionOnePOM := fixtureAddition(
		"al.aldi", "sprova4j", "0.1.1", "", "pom", "jar", "sprova4j", sprovaDescription,
		1635883814509, 1626111437014, -1, true,
	)
	secondIncrement := []Event{
		fixtureAddition(
			"com.jolira", "wicket-guicier-parent", "2.0.12", "", "pom", "pom", "Wicket Guicier Parent",
			"A resplacement for wicket-guice that uses constructor injection as an alternative to the excessive use of PageParameters.",
			1635883823271, 1320803683000, 2940, false,
		),
		fixtureRemoval("0.1.0", "", 1635883823411),
		fixtureRemoval("0.1.0", "sources", 1635883823414),
	}
	full := append([]Event{
		fixtureAddition(
			"al.aldi", "sprova4j", "0.1.0", "sources", "jar", "jar", "sprova4j", sprovaDescription,
			1635883794419, 1626109619335, 14316, false,
		),
		fixtureAddition(
			"al.aldi", "sprova4j", "0.1.0", "", "pom", "jar", "sprova4j", sprovaDescription,
			1635883794437, 1626109636636, -1, true,
		),
		versionOneSources,
		versionOnePOM,
	}, secondIncrement...)

	tests := []struct {
		name      string
		filename  string
		timestamp int64
		want      []Event
	}{
		{"full", "nexus-maven-repository-index.gz", 1635883823455, full},
		{"incremental 1", "nexus-maven-repository-index.1.gz", 1635883814651, []Event{versionOneSources, versionOnePOM}},
		{"incremental 2", "nexus-maven-repository-index.2.gz", 1635883823455, secondIncrement},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input, err := os.Open(filepath.Join(mavenExporterFixtureDirectory, test.filename))
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestReader(t, input)
			reader, err := NewReader(input, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestReader(t, reader)
			if got := reader.Header().Timestamp.UnixMilli(); got != test.timestamp {
				t.Errorf("header timestamp = %d, want %d", got, test.timestamp)
			}

			got := readFixtureEvents(t, reader)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("events =\n%+v\nwant =\n%+v", got, test.want)
			}
		})
	}
}

func TestMavenIndexerCompatibilityFixtureMarkers(t *testing.T) {
	input, err := os.Open(filepath.Join(mavenExporterFixtureDirectory, "nexus-maven-repository-index.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, input)
	reader, err := NewRawReader(input, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)

	var records []Record
	for {
		record, nextErr := reader.NextRecord()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		records = append(records, record)
	}
	if len(records) != 10 {
		t.Fatalf("raw records = %d, want 10", len(records))
	}
	markers := []struct {
		record int
		name   string
		value  string
	}{
		{7, "DESCRIPTOR", "NexusIndex"},
		{8, fieldAllGroups, fieldAllGroups},
		{9, "rootGroups", "rootGroups"},
	}
	for _, marker := range markers {
		if value, ok := records[marker.record].Value(marker.name); !ok || value != marker.value {
			t.Errorf("record %d field %q = %q, %v", marker.record, marker.name, value, ok)
		}
	}
}

func TestExternalApacheMavenIndexerFixture(t *testing.T) {
	path := os.Getenv("NEXUS_APACHE_INDEX_FIXTURE")
	if path == "" {
		t.Skip("set NEXUS_APACHE_INDEX_FIXTURE to an Apache Maven Indexer chunk")
	}
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, input)
	reader, err := NewReader(input, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestReader(t, reader)
	readFixtureEvents(t, reader)
}

func readFixtureEvents(t testing.TB, reader *Reader) []Event {
	t.Helper()
	var events []Event
	for {
		event, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func fixtureAddition(
	groupID, artifactID, version, classifier, extension, packaging, name, description string,
	modifiedMillis, fileModifiedMillis, fileSize int64,
	hasSources bool,
) Event {
	return Event{Type: Add, Artifact: Artifact{
		GroupID:      groupID,
		ArtifactID:   artifactID,
		Version:      version,
		Classifier:   classifier,
		Extension:    extension,
		Packaging:    packaging,
		Name:         name,
		Description:  description,
		ModifiedAt:   time.UnixMilli(modifiedMillis).UTC(),
		FileModified: time.UnixMilli(fileModifiedMillis).UTC(),
		FileSize:     fileSize,
		HasSources:   hasSources,
	}}
}

func fixtureRemoval(version, classifier string, modifiedMillis int64) Event {
	return Event{Type: Remove, Artifact: Artifact{
		GroupID:    "al.aldi",
		ArtifactID: "sprova4j",
		Version:    version,
		Classifier: classifier,
		ModifiedAt: time.UnixMilli(modifiedMillis).UTC(),
	}}
}
