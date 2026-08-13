package nexus

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	propertiesPath = ".index/nexus-maven-repository-index.properties"
	fullChunkPath  = ".index/nexus-maven-repository-index.gz"
)

// SyncMode describes how a remote index should be applied.
type SyncMode string

const (
	// SyncCurrent means the caller's cursor already describes the remote index.
	SyncCurrent SyncMode = "current"
	// SyncFull means the caller must replace any previous catalog state.
	SyncFull SyncMode = "full"
	// SyncIncremental means the chunks advance the caller's existing state.
	SyncIncremental SyncMode = "incremental"
)

// Cursor records an index state successfully applied by a caller.
type Cursor struct {
	IndexID         string    `json:"index_id"`
	ChainID         string    `json:"chain_id,omitempty"`
	LastIncremental *int64    `json:"last_incremental,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
	ETag            string    `json:"etag,omitempty"`
	LastModified    string    `json:"last_modified,omitempty"`
}

// ChunkRef identifies a chunk selected for download.
type ChunkRef struct {
	Path        string `json:"path"`
	Incremental bool   `json:"incremental"`
	Counter     int64  `json:"counter,omitempty"`
}

// SyncPlan is the result of comparing remote index properties with a cursor.
type SyncPlan struct {
	Mode   SyncMode   `json:"mode"`
	Chunks []ChunkRef `json:"chunks"`
	Target Cursor     `json:"target"`
}

// PropertiesPath is the repository-relative path to the index properties
// file.
func PropertiesPath() string {
	return propertiesPath
}

// BuildPlan chooses a full chunk, an incremental sequence, or no work.
func BuildPlan(remote IndexProperties, cursor *Cursor) (SyncPlan, error) {
	normalized, err := normalizeProperties(remote)
	if err != nil {
		return SyncPlan{}, err
	}
	target := cursorFromProperties(normalized)

	if cursor == nil {
		return fullPlan(target), nil
	}
	if err := validateCursor(*cursor); err != nil {
		return SyncPlan{}, err
	}
	if cursor.IndexID != normalized.ID {
		return fullPlan(target), nil
	}

	remoteHasChain := normalized.ChainID != "" && normalized.LastIncremental != nil
	if !remoteHasChain {
		if cursor.Timestamp.Equal(normalized.Timestamp) {
			return currentPlan(target), nil
		}
		return fullPlan(target), nil
	}

	if cursor.ChainID != "" && cursor.ChainID != normalized.ChainID {
		return fullPlan(target), nil
	}
	if cursor.ChainID == "" || cursor.LastIncremental == nil {
		if cursor.Timestamp.Equal(normalized.Timestamp) {
			return currentPlan(target), nil
		}
		return fullPlan(target), nil
	}

	from := *cursor.LastIncremental
	to := *normalized.LastIncremental
	if from == to {
		return currentPlan(target), nil
	}
	if from > to {
		return fullPlan(target), nil
	}

	chunks, complete := incrementalChunks(normalized.Incrementals, from, to)
	if !complete {
		return fullPlan(target), nil
	}
	return SyncPlan{Mode: SyncIncremental, Chunks: chunks, Target: target}, nil
}

func normalizeProperties(properties IndexProperties) (IndexProperties, error) {
	if properties.ID == "" {
		return IndexProperties{}, errors.New("nexus: remote index ID is empty")
	}
	if properties.Timestamp.IsZero() {
		return IndexProperties{}, errors.New("nexus: remote index timestamp is zero")
	}
	if properties.LastIncremental != nil && *properties.LastIncremental < 0 {
		return IndexProperties{}, errors.New("nexus: remote last incremental must not be negative")
	}

	normalized := properties
	normalized.Incrementals = append([]int64(nil), properties.Incrementals...)
	sort.Slice(normalized.Incrementals, func(left, right int) bool {
		return normalized.Incrementals[left] < normalized.Incrementals[right]
	})
	for index, counter := range normalized.Incrementals {
		if counter < 0 {
			return IndexProperties{}, fmt.Errorf("nexus: remote incremental must not be negative: %d", counter)
		}
		if index > 0 && counter == normalized.Incrementals[index-1] {
			return IndexProperties{}, fmt.Errorf("nexus: remote incremental %d is duplicated", counter)
		}
		if normalized.LastIncremental == nil {
			return IndexProperties{}, errors.New("nexus: remote incrementals have no last incremental")
		}
		if counter > *normalized.LastIncremental {
			return IndexProperties{}, fmt.Errorf("nexus: remote incremental %d is ahead of last incremental %d", counter, *normalized.LastIncremental)
		}
	}
	return normalized, nil
}

func validateCursor(cursor Cursor) error {
	if cursor.IndexID == "" {
		return errors.New("nexus: cursor index ID is empty")
	}
	if cursor.LastIncremental != nil && *cursor.LastIncremental < 0 {
		return errors.New("nexus: cursor last incremental must not be negative")
	}
	return nil
}

func cursorFromProperties(properties IndexProperties) Cursor {
	cursor := Cursor{
		IndexID:   properties.ID,
		ChainID:   properties.ChainID,
		Timestamp: properties.Timestamp,
	}
	if properties.LastIncremental != nil {
		last := *properties.LastIncremental
		cursor.LastIncremental = &last
	}
	return cursor
}

func currentPlan(target Cursor) SyncPlan {
	return SyncPlan{Mode: SyncCurrent, Chunks: []ChunkRef{}, Target: target}
}

func fullPlan(target Cursor) SyncPlan {
	return SyncPlan{
		Mode: SyncFull,
		Chunks: []ChunkRef{{
			Path: fullChunkPath,
		}},
		Target: target,
	}
}

func incrementalChunks(available []int64, from, to int64) ([]ChunkRef, bool) {
	if from == math.MaxInt64 {
		return nil, false
	}
	next := from + 1
	chunks := make([]ChunkRef, 0, len(available))
	for _, counter := range available {
		if counter < next {
			continue
		}
		if counter != next {
			return nil, false
		}
		chunks = append(chunks, ChunkRef{
			Path:        fmt.Sprintf(".index/nexus-maven-repository-index.%d.gz", counter),
			Incremental: true,
			Counter:     counter,
		})
		if counter == to {
			return chunks, true
		}
		if counter == math.MaxInt64 {
			return nil, false
		}
		next = counter + 1
	}
	return nil, false
}
