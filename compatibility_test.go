package nexus

import (
	"errors"
	"io"
	"os"
	"testing"
)

func TestApacheMavenIndexerFixture(t *testing.T) {
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

	events := 0
	for {
		_, err = reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		events++
	}
	if events != 2 {
		t.Errorf("artifact events = %d, want 2", events)
	}
}
