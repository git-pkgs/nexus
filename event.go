package nexus

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// EventType identifies an artifact addition or removal.
type EventType string

const (
	// Add identifies an artifact addition.
	Add EventType = "add"
	// Remove identifies an artifact removal.
	Remove EventType = "remove"

	fieldUInfo       = "u"
	fieldDeleted     = "del"
	fieldInfo        = "i"
	fieldModified    = "m"
	fieldName        = "n"
	fieldDescription = "d"
	fieldSHA1        = "1"
	fieldAllGroups   = "allGroups"
	packagingPOM     = "pom"
	packagingWAR     = "war"
	packagingEAR     = "ear"
	packagingJAR     = "jar"
)

// Artifact is the catalog data retained from one index record.
type Artifact struct {
	GroupID      string    `json:"group_id"`
	ArtifactID   string    `json:"artifact_id"`
	Version      string    `json:"version"`
	Classifier   string    `json:"classifier,omitempty"`
	Extension    string    `json:"extension,omitempty"`
	Packaging    string    `json:"packaging,omitempty"`
	Name         string    `json:"name,omitempty"`
	Description  string    `json:"description,omitempty"`
	SHA1         string    `json:"sha1,omitempty"`
	ModifiedAt   time.Time `json:"modified_at,omitzero"`
	FileModified time.Time `json:"file_modified,omitzero"`
	FileSize     int64     `json:"file_size,omitempty"`
	HasSources   bool      `json:"has_sources,omitempty"`
	HasJavadoc   bool      `json:"has_javadoc,omitempty"`
	HasSignature bool      `json:"has_signature,omitempty"`
}

// Event is one typed artifact change from a chunk.
type Event struct {
	Type     EventType `json:"type"`
	Artifact Artifact  `json:"artifact"`
}

// Reader maps raw chunk records to artifact events.
type Reader struct {
	raw      *RawReader
	terminal error
}

// NewReader opens a chunk and returns its typed artifact event reader.
func NewReader(input io.Reader, options Options) (*Reader, error) {
	raw, err := NewRawReader(input, options)
	if err != nil {
		return nil, err
	}
	return &Reader{raw: raw}, nil
}

// Header returns the chunk header read by NewReader.
func (reader *Reader) Header() Header {
	return reader.raw.Header()
}

// Next returns the next artifact event, skipping descriptor and group records.
func (reader *Reader) Next() (Event, error) {
	if reader.terminal != nil {
		return Event{}, reader.terminal
	}
	for {
		record, err := reader.raw.nextRecord(eventFieldSelected)
		if err != nil {
			return Event{}, err
		}
		event, emitted, mapErr := mapRecord(record)
		if mapErr != nil {
			reader.terminal = mapErr
			return Event{}, reader.terminal
		}
		if emitted {
			return event, nil
		}
	}
}

func eventFieldSelected(name string) bool {
	switch name {
	case fieldUInfo, fieldDeleted, fieldInfo, fieldModified, fieldName, fieldDescription, fieldSHA1:
		return true
	default:
		return false
	}
}

// Close releases parser-owned resources without closing the caller's input.
func (reader *Reader) Close() error {
	return reader.raw.Close()
}

type recordValues struct {
	uinfo       string
	hasUinfo    bool
	deleted     string
	hasDeleted  bool
	info        string
	hasInfo     bool
	modified    string
	hasModified bool
	name        string
	description string
	sha1        string
}

func mapRecord(record Record) (Event, bool, error) {
	var values recordValues
	for _, field := range record.Fields {
		switch field.Name {
		case fieldUInfo:
			values.uinfo, values.hasUinfo = field.Value, true
		case fieldDeleted:
			values.deleted, values.hasDeleted = field.Value, true
		case fieldInfo:
			values.info, values.hasInfo = field.Value, true
		case fieldModified:
			values.modified, values.hasModified = field.Value, true
		case fieldName:
			values.name = field.Value
		case fieldDescription:
			values.description = field.Value
		case fieldSHA1:
			values.sha1 = field.Value
		}
	}

	if values.hasDeleted {
		artifact, err := parseIdentity(values.deleted, true)
		if err != nil {
			return Event{}, false, fmt.Errorf("nexus: map removal: %w", err)
		}
		if values.hasModified {
			modified, modifiedErr := parseOptionalMillis("record modification", values.modified)
			if modifiedErr != nil {
				return Event{}, false, modifiedErr
			}
			artifact.ModifiedAt = modified
		}
		return Event{Type: Remove, Artifact: artifact}, true, nil
	}
	if !values.hasUinfo {
		return Event{}, false, nil
	}

	artifact, err := parseIdentity(values.uinfo, false)
	if err != nil {
		return Event{}, false, fmt.Errorf("nexus: map addition: %w", err)
	}
	if values.hasInfo {
		if err := applyInfo(&artifact, values.info); err != nil {
			return Event{}, false, err
		}
	}
	if values.hasModified {
		modified, modifiedErr := parseOptionalMillis("record modification", values.modified)
		if modifiedErr != nil {
			return Event{}, false, modifiedErr
		}
		artifact.ModifiedAt = modified
	}
	artifact.Name = values.name
	artifact.Description = values.description
	artifact.SHA1 = values.sha1
	return Event{Type: Add, Artifact: artifact}, true, nil
}

func parseIdentity(value string, removal bool) (Artifact, error) {
	parts := strings.Split(value, "|")
	if len(parts) != 4 && len(parts) != 5 {
		return Artifact{}, fmt.Errorf("artifact identity has %d parts, want 4 or 5", len(parts))
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Artifact{}, errors.New("artifact identity has an empty group ID, artifact ID, or version")
	}
	artifact := Artifact{
		GroupID:    parts[0],
		ArtifactID: parts[1],
		Version:    parts[2],
	}
	if parts[3] != "NA" {
		artifact.Classifier = parts[3]
	}
	if len(parts) == 5 && parts[4] != "NA" && (!removal || parts[4] != "null") {
		artifact.Extension = parts[4]
	}
	return artifact, nil
}

func applyInfo(artifact *Artifact, value string) error {
	parts := strings.Split(value, "|")
	if len(parts) != 6 && len(parts) != 7 {
		return fmt.Errorf("nexus: artifact info has %d parts, want 6 or 7", len(parts))
	}
	if parts[0] != "NA" {
		artifact.Packaging = parts[0]
	}

	fileModified, err := parseOptionalMillis("file modification", parts[1])
	if err != nil {
		return err
	}
	artifact.FileModified = fileModified
	fileSize, err := parseOptionalInt64("file size", parts[2])
	if err != nil {
		return err
	}
	artifact.FileSize = fileSize
	artifact.HasSources, err = parseInfoFlag("sources", parts[3])
	if err != nil {
		return err
	}
	artifact.HasJavadoc, err = parseInfoFlag("Javadoc", parts[4])
	if err != nil {
		return err
	}
	artifact.HasSignature, err = parseInfoFlag("signature", parts[5])
	if err != nil {
		return err
	}

	extension := ""
	if len(parts) == 7 && parts[6] != "NA" {
		extension = parts[6]
	}
	if extension == "" {
		if artifact.Classifier != "" || artifact.Packaging == packagingPOM || artifact.Packaging == packagingWAR || artifact.Packaging == packagingEAR {
			extension = artifact.Packaging
		} else {
			extension = packagingJAR
		}
	}
	artifact.Extension = extension
	return nil
}

func parseOptionalMillis(name, value string) (time.Time, error) {
	parsed, err := parseOptionalInt64(name, value)
	if err != nil {
		return time.Time{}, err
	}
	if value == "" || value == "NA" {
		return time.Time{}, nil
	}
	return time.UnixMilli(parsed).UTC(), nil
}

func parseOptionalInt64(name, value string) (int64, error) {
	if value == "" || value == "NA" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("nexus: parse %s %q: %w", name, value, err)
	}
	return parsed, nil
}

func parseInfoFlag(name, value string) (bool, error) {
	switch value {
	case "", "0", "NA":
		return false, nil
	case "1":
		return true, nil
	case "2":
		// Maven Indexer uses 2 for ArtifactAvailability.NOT_AVAILABLE.
		return false, nil
	default:
		return false, fmt.Errorf("nexus: parse %s flag %q: want 0, 1, or 2", name, value)
	}
}
