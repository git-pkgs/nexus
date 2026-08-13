package nexus

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testIndexID = "index"
	testChainID = "chain"
)

func TestBuildPlanColdStart(t *testing.T) {
	remote := testProperties(testIndexID, testChainID, 12, []int64{10, 11, 12})

	plan, err := BuildPlan(remote, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncFull {
		t.Fatalf("Mode = %q, want %q", plan.Mode, SyncFull)
	}
	want := []ChunkRef{{Path: fullChunkPath}}
	if !reflect.DeepEqual(plan.Chunks, want) {
		t.Errorf("Chunks = %+v, want %+v", plan.Chunks, want)
	}
	if plan.Target.LastIncremental == nil || *plan.Target.LastIncremental != 12 {
		t.Errorf("target = %+v", plan.Target)
	}
}

func TestBuildPlanCurrent(t *testing.T) {
	remote := testProperties(testIndexID, testChainID, 12, []int64{10, 11, 12})
	cursor := testCursor(testIndexID, testChainID, 12, remote.Timestamp)

	plan, err := BuildPlan(remote, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncCurrent || len(plan.Chunks) != 0 {
		t.Errorf("plan = %+v", plan)
	}
}

func TestBuildPlanIncremental(t *testing.T) {
	remote := testProperties(testIndexID, testChainID, 12, []int64{12, 10, 11, 8})
	cursor := testCursor(testIndexID, testChainID, 9, remote.Timestamp.Add(-time.Hour))
	original := append([]int64(nil), remote.Incrementals...)

	plan, err := BuildPlan(remote, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncIncremental {
		t.Fatalf("Mode = %q, want %q", plan.Mode, SyncIncremental)
	}
	want := []ChunkRef{
		{Path: ".index/nexus-maven-repository-index.10.gz", Incremental: true, Counter: 10},
		{Path: ".index/nexus-maven-repository-index.11.gz", Incremental: true, Counter: 11},
		{Path: ".index/nexus-maven-repository-index.12.gz", Incremental: true, Counter: 12},
	}
	if !reflect.DeepEqual(plan.Chunks, want) {
		t.Errorf("Chunks = %+v, want %+v", plan.Chunks, want)
	}
	if !reflect.DeepEqual(remote.Incrementals, original) {
		t.Errorf("BuildPlan mutated remote increments: %v", remote.Incrementals)
	}
}

func TestBuildPlanFallsBackToFull(t *testing.T) {
	remote := testProperties(testIndexID, testChainID, 12, []int64{10, 11, 12})
	tests := []struct {
		name   string
		cursor Cursor
	}{
		{"changed index", testCursor("other", testChainID, 9, remote.Timestamp.Add(-time.Hour))},
		{"changed chain", testCursor(testIndexID, "other", 9, remote.Timestamp.Add(-time.Hour))},
		{"changed chain at same timestamp", testCursor(testIndexID, "other", 9, remote.Timestamp)},
		{"missing counter", testCursor(testIndexID, testChainID, 8, remote.Timestamp.Add(-time.Hour))},
		{"cursor ahead", testCursor(testIndexID, testChainID, 13, remote.Timestamp.Add(-time.Hour))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := BuildPlan(remote, &test.cursor)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Mode != SyncFull || len(plan.Chunks) != 1 || plan.Chunks[0].Path != fullChunkPath {
				t.Errorf("plan = %+v", plan)
			}
		})
	}
}

func TestBuildPlanWithoutIncrementalMetadata(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 3, 8, 42, 0, time.UTC)
	remote := IndexProperties{ID: testIndexID, Timestamp: timestamp}

	current := Cursor{IndexID: testIndexID, Timestamp: timestamp}
	plan, err := BuildPlan(remote, &current)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncCurrent {
		t.Errorf("current plan = %+v", plan)
	}

	stale := Cursor{IndexID: testIndexID, Timestamp: timestamp.Add(-time.Hour)}
	plan, err = BuildPlan(remote, &stale)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncFull {
		t.Errorf("stale plan = %+v", plan)
	}
}

func TestBuildPlanMatchingTimestampCanAdoptChain(t *testing.T) {
	remote := testProperties(testIndexID, testChainID, 12, []int64{10, 11, 12})
	cursor := Cursor{IndexID: testIndexID, Timestamp: remote.Timestamp}

	plan, err := BuildPlan(remote, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != SyncCurrent {
		t.Errorf("plan = %+v", plan)
	}
	if plan.Target.ChainID != testChainID || plan.Target.LastIncremental == nil || *plan.Target.LastIncremental != 12 {
		t.Errorf("target = %+v", plan.Target)
	}
}

func TestBuildPlanRejectsInvalidInput(t *testing.T) {
	timestamp := time.Date(2026, time.August, 13, 3, 8, 42, 0, time.UTC)
	negative := int64(-1)
	tests := []struct {
		name   string
		remote IndexProperties
		cursor *Cursor
		want   string
	}{
		{"empty remote ID", IndexProperties{Timestamp: timestamp}, nil, "index ID"},
		{"zero remote timestamp", IndexProperties{ID: testIndexID}, nil, testTimestampWord},
		{"negative remote counter", IndexProperties{ID: testIndexID, Timestamp: timestamp, LastIncremental: &negative}, nil, negativeCounterError},
		{"increment without last", IndexProperties{ID: testIndexID, Timestamp: timestamp, Incrementals: []int64{1}}, nil, "no last incremental"},
		{"empty cursor ID", IndexProperties{ID: testIndexID, Timestamp: timestamp}, &Cursor{}, "cursor index ID"},
		{"negative cursor counter", IndexProperties{ID: testIndexID, Timestamp: timestamp}, &Cursor{IndexID: testIndexID, LastIncremental: &negative}, negativeCounterError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildPlan(test.remote, test.cursor)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func testProperties(id, chain string, last int64, increments []int64) IndexProperties {
	return IndexProperties{
		ID:              id,
		Timestamp:       time.Date(2026, time.August, 13, 3, 8, 42, 0, time.UTC),
		ChainID:         chain,
		LastIncremental: int64Pointer(last),
		Incrementals:    increments,
	}
}

func testCursor(id, chain string, last int64, timestamp time.Time) Cursor {
	return Cursor{
		IndexID:         id,
		ChainID:         chain,
		LastIncremental: int64Pointer(last),
		Timestamp:       timestamp,
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}
