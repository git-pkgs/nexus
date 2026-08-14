package nexus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
)

var (
	// ErrCheckpointUnavailable is returned before a chunk reaches verified EOF.
	ErrCheckpointUnavailable = errors.New("nexus: checkpoint is not available before verified chunk EOF")
	// ErrChunkActive is returned when another chunk is still open.
	ErrChunkActive = errors.New("nexus: a chunk is already active")
	// ErrChunkIncomplete is returned after an active chunk is closed early.
	ErrChunkIncomplete = errors.New("nexus: synchronization chunk was not fully consumed")
	// ErrSyncClosed is returned after a synchronization is closed.
	ErrSyncClosed = errors.New("nexus: synchronization is closed")
	// ErrSyncPlanChanged is returned when refreshed properties would change the
	// mode, target, or remaining chunks of an active synchronization. Start a
	// new synchronization with the latest committed checkpoint.
	ErrSyncPlanChanged = errors.New("nexus: synchronization plan changed")
)

type propertiesResponse struct {
	properties   IndexProperties
	etag         string
	lastModified string
	notModified  bool
}

// Sync streams the chunks selected for one repository synchronization.
type Sync struct {
	client     *Client
	ctx        context.Context
	cancel     context.CancelFunc
	repository *url.URL
	plan       SyncPlan
	cursor     *Cursor
	position   int
	current    *Chunk
	terminal   error
	refreshed  bool
	closed     bool
}

// Chunk streams typed events from one downloaded index chunk.
type Chunk struct {
	parent     *Sync
	ref        ChunkRef
	reader     *Reader
	body       io.ReadCloser
	header     Header
	checkpoint Cursor
	complete   bool
	closed     bool
}

// Sync fetches index properties, selects the required chunks, and prepares a
// serial event stream.
func (client *Client) Sync(ctx context.Context, repositoryURL string, cursor *Cursor) (*Sync, error) {
	if client == nil {
		return nil, errors.New("nexus: nil client")
	}
	if client.configErr != nil {
		return nil, client.configErr
	}
	if ctx == nil {
		return nil, errors.New("nexus: nil synchronization context")
	}
	if cursor != nil {
		if err := validateCursor(*cursor); err != nil {
			return nil, err
		}
	}
	repository, err := validateRepositoryURL(repositoryURL)
	if err != nil {
		return nil, err
	}
	syncContext, cancel := context.WithCancel(ctx)
	properties, err := client.fetchProperties(syncContext, repository, cursor)
	if err != nil {
		cancel()
		return nil, err
	}

	if properties.notModified {
		if cursor == nil {
			cancel()
			return nil, errors.New("nexus: unexpected 304 response without a cursor")
		}
		current := cloneCursor(*cursor)
		return &Sync{
			client:     client,
			ctx:        syncContext,
			cancel:     cancel,
			repository: repository,
			plan:       SyncPlan{Mode: SyncCurrent, Chunks: []ChunkRef{}, Target: cloneCursor(*cursor)},
			cursor:     &current,
		}, nil
	}

	plan, err := BuildPlan(properties.properties, cursor)
	if err != nil {
		cancel()
		return nil, err
	}
	plan.Target.ETag = properties.etag
	plan.Target.LastModified = properties.lastModified
	sync := &Sync{
		client:     client,
		ctx:        syncContext,
		cancel:     cancel,
		repository: repository,
		plan:       plan,
	}
	if cursor != nil {
		current := cloneCursor(*cursor)
		sync.cursor = &current
	}
	return sync, nil
}

func (client *Client) fetchProperties(ctx context.Context, repository *url.URL, cursor *Cursor) (propertiesResponse, error) {
	remote := resolveRepositoryPath(repository, propertiesPath)
	headers := make(http.Header)
	if cursor != nil {
		if cursor.ETag != "" {
			headers.Set("If-None-Match", cursor.ETag)
		}
		if cursor.LastModified != "" {
			headers.Set("If-Modified-Since", cursor.LastModified)
		}
	}
	response, err := client.do(ctx, remote, headers)
	if err != nil {
		return propertiesResponse{}, fmt.Errorf("nexus: fetch index properties: %w", err)
	}
	defer closeResponse(response)

	if response.StatusCode == http.StatusNotModified {
		return propertiesResponse{notModified: true}, nil
	}
	if response.StatusCode == http.StatusNotFound {
		return propertiesResponse{}, fmt.Errorf("%w: %s", ErrIndexNotFound, remote)
	}
	if response.StatusCode != http.StatusOK {
		return propertiesResponse{}, statusError(response)
	}
	if response.ContentLength > client.options.maxPropertiesBytes {
		return propertiesResponse{}, fmt.Errorf("%w: content length is %d, maximum is %d", ErrPropertiesTooLarge, response.ContentLength, client.options.maxPropertiesBytes)
	}
	properties, err := ParseProperties(response.Body, PropertiesOptions{MaxBytes: client.options.maxPropertiesBytes})
	if err != nil {
		return propertiesResponse{}, err
	}
	return propertiesResponse{
		properties:   properties,
		etag:         response.Header.Get("ETag"),
		lastModified: response.Header.Get("Last-Modified"),
	}, nil
}

// Mode reports whether this synchronization is current, full, or incremental.
func (sync *Sync) Mode() SyncMode {
	return sync.plan.Mode
}

// Target returns the remote cursor this synchronization will reach after all
// selected chunks are applied.
func (sync *Sync) Target() Cursor {
	return cloneCursor(sync.plan.Target)
}

// NextChunk downloads and opens the next selected chunk. A chunk must reach
// verified EOF, or be closed, before another can be opened.
func (sync *Sync) NextChunk() (*Chunk, error) {
	if sync.closed {
		return nil, ErrSyncClosed
	}
	if sync.terminal != nil {
		return nil, sync.terminal
	}
	if sync.current != nil {
		return nil, ErrChunkActive
	}
	for {
		if sync.position >= len(sync.plan.Chunks) {
			return nil, io.EOF
		}

		ref := sync.plan.Chunks[sync.position]
		remote := resolveRepositoryPath(sync.repository, ref.Path)
		response, err := sync.client.do(sync.ctx, remote, nil)
		if err != nil {
			return nil, fmt.Errorf("nexus: fetch chunk %s: %w", ref.Path, err)
		}
		if response.StatusCode == http.StatusNotFound && !sync.refreshed {
			closeResponse(response)
			if err := sync.refreshPlan(); err != nil {
				return nil, err
			}
			continue
		}
		if response.StatusCode != http.StatusOK {
			err := statusError(response)
			closeResponse(response)
			return nil, err
		}
		body, err := boundedBody(response, sync.client.options.maxCompressedBytes)
		if err != nil {
			return nil, err
		}
		reader, err := NewReader(body, sync.client.options.parserOptions)
		if err != nil {
			_ = body.Close()
			return nil, fmt.Errorf("nexus: open chunk %s: %w", ref.Path, err)
		}
		chunk := &Chunk{
			parent: sync,
			ref:    ref,
			reader: reader,
			body:   body,
			header: reader.Header(),
		}
		chunk.checkpoint = sync.checkpointFor(ref, chunk.header)
		sync.current = chunk
		return chunk, nil
	}
}

func (sync *Sync) refreshPlan() error {
	sync.refreshed = true
	properties, err := sync.client.fetchProperties(sync.ctx, sync.repository, nil)
	if err != nil {
		return fmt.Errorf("nexus: refresh after missing chunk: %w", err)
	}
	plan, err := BuildPlan(properties.properties, sync.cursor)
	if err != nil {
		return fmt.Errorf("nexus: replan after missing chunk: %w", err)
	}
	plan.Target.ETag = properties.etag
	plan.Target.LastModified = properties.lastModified
	if !sameRefreshPlan(sync.plan, sync.position, plan) {
		err := fmt.Errorf("%w: start a new synchronization with the latest committed checkpoint", ErrSyncPlanChanged)
		sync.terminal = err
		return err
	}
	sync.plan = plan
	sync.position = 0
	return nil
}

func sameRefreshPlan(current SyncPlan, position int, refreshed SyncPlan) bool {
	return current.Mode == refreshed.Mode &&
		cursorEqual(current.Target, refreshed.Target) &&
		slices.Equal(current.Chunks[position:], refreshed.Chunks)
}

func cursorEqual(left, right Cursor) bool {
	if left.IndexID != right.IndexID ||
		left.ChainID != right.ChainID ||
		!left.Timestamp.Equal(right.Timestamp) ||
		left.ETag != right.ETag ||
		left.LastModified != right.LastModified {
		return false
	}
	if left.LastIncremental == nil || right.LastIncremental == nil {
		return left.LastIncremental == nil && right.LastIncremental == nil
	}
	return *left.LastIncremental == *right.LastIncremental
}

func (sync *Sync) checkpointFor(ref ChunkRef, header Header) Cursor {
	if sync.plan.Mode == SyncFull {
		return cloneCursor(sync.plan.Target)
	}
	if sync.plan.Target.LastIncremental != nil && ref.Counter == *sync.plan.Target.LastIncremental {
		return cloneCursor(sync.plan.Target)
	}
	counter := ref.Counter
	return Cursor{
		IndexID:         sync.plan.Target.IndexID,
		ChainID:         sync.plan.Target.ChainID,
		LastIncremental: &counter,
		Timestamp:       header.Timestamp,
	}
}

func (sync *Sync) finishChunk(chunk *Chunk, complete bool) {
	if sync.current != chunk {
		return
	}
	sync.current = nil
	if complete {
		checkpoint := cloneCursor(chunk.checkpoint)
		sync.cursor = &checkpoint
		sync.position++
		return
	}
	sync.terminal = ErrChunkIncomplete
}

// Close cancels outstanding requests and closes an active chunk.
func (sync *Sync) Close() error {
	if sync.closed {
		return nil
	}
	sync.closed = true
	var err error
	if sync.current != nil {
		err = sync.current.Close()
	}
	sync.cancel()
	return err
}

// Header returns this chunk's binary header.
func (chunk *Chunk) Header() Header {
	return chunk.header
}

// Ref returns the selected path and incremental counter for this chunk.
func (chunk *Chunk) Ref() ChunkRef {
	return chunk.ref
}

// Next returns the next typed artifact event.
func (chunk *Chunk) Next() (Event, error) {
	if chunk.closed {
		if chunk.complete {
			return Event{}, io.EOF
		}
		return Event{}, ErrChunkIncomplete
	}
	event, err := chunk.reader.Next()
	if errors.Is(err, io.EOF) {
		if closeErr := chunk.finish(true); closeErr != nil {
			return Event{}, closeErr
		}
		return Event{}, io.EOF
	}
	if err != nil {
		_ = chunk.finish(false)
		return Event{}, err
	}
	return event, nil
}

// Checkpoint returns the cursor unlocked by a clean gzip EOF.
func (chunk *Chunk) Checkpoint() (Cursor, error) {
	if !chunk.complete {
		return Cursor{}, ErrCheckpointUnavailable
	}
	return cloneCursor(chunk.checkpoint), nil
}

// Close releases the response body. Closing before verified EOF aborts the
// synchronization and does not unlock a checkpoint.
func (chunk *Chunk) Close() error {
	if chunk.closed {
		return nil
	}
	return chunk.finish(false)
}

func (chunk *Chunk) finish(complete bool) error {
	if chunk.closed {
		return nil
	}
	chunk.closed = true
	readerErr := chunk.reader.Close()
	bodyErr := chunk.body.Close()
	chunk.complete = complete && readerErr == nil && bodyErr == nil
	chunk.parent.finishChunk(chunk, chunk.complete)
	return errors.Join(readerErr, bodyErr)
}

func cloneCursor(cursor Cursor) Cursor {
	clone := cursor
	if cursor.LastIncremental != nil {
		counter := *cursor.LastIncremental
		clone.LastIncremental = &counter
	}
	return clone
}
