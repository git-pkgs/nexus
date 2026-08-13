package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/git-pkgs/nexus"
)

const (
	maxCursorBytes int64 = 1 << 20
	parseCommand         = "parse"
	syncCommand          = "sync"
)

type syncRecord struct {
	Type    string         `json:"type"`
	Mode    nexus.SyncMode `json:"mode"`
	IndexID string         `json:"index_id"`
	From    *int64         `json:"from,omitempty"`
	To      *int64         `json:"to,omitempty"`
}

type eventRecord struct {
	Type nexus.EventType `json:"type"`
	nexus.Artifact
}

type checkpointRecord struct {
	Type   string       `json:"type"`
	Cursor nexus.Cursor `json:"cursor"`
}

func main() {
	os.Exit(realMain())
}

func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: nexus <parse|sync> [options]")
	}
	switch args[0] {
	case parseCommand:
		return runParse(args[1:], stdin, stdout, stderr)
	case syncCommand:
		return runSync(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("nexus: unknown command %q", args[0])
	}
}

func runParse(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("parse", flag.ContinueOnError)
	flags.SetOutput(stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: nexus parse <chunk.gz|->")
	}

	input := stdin
	var file *os.File
	if flags.Arg(0) != "-" {
		opened, err := os.Open(flags.Arg(0))
		if err != nil {
			return fmt.Errorf("nexus: open chunk: %w", err)
		}
		file = opened
		input = opened
		defer func() { _ = file.Close() }()
	}

	reader, err := nexus.NewReader(input, nexus.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	return writeEvents(json.NewEncoder(stdout), reader.Next)
}

func runSync(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cursorPath := flags.String("cursor", "", "read the previous cursor from a JSON file")
	allowPrivate := flags.Bool("allow-private", false, "allow private and loopback repository addresses")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: nexus sync [--cursor cursor.json] [--allow-private] <repository-url>")
	}

	cursor, err := readCursor(*cursorPath)
	if err != nil {
		return err
	}
	client := nexus.NewClient(nexus.ClientOptions{AllowPrivateAddresses: *allowPrivate})
	synchronization, err := client.Sync(ctx, flags.Arg(0), cursor)
	if err != nil {
		return err
	}
	defer func() { _ = synchronization.Close() }()

	target := synchronization.Target()
	control := syncRecord{
		Type:    "sync",
		Mode:    synchronization.Mode(),
		IndexID: target.IndexID,
		To:      cloneCounter(target.LastIncremental),
	}
	if cursor != nil {
		control.From = cloneCounter(cursor.LastIncremental)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(control); err != nil {
		return fmt.Errorf("nexus: write sync record: %w", err)
	}

	for {
		chunk, nextErr := synchronization.NextChunk()
		if errors.Is(nextErr, io.EOF) {
			return nil
		}
		if nextErr != nil {
			return nextErr
		}
		if err := writeEvents(encoder, chunk.Next); err != nil {
			return err
		}
		checkpoint, checkpointErr := chunk.Checkpoint()
		if checkpointErr != nil {
			return checkpointErr
		}
		if err := encoder.Encode(checkpointRecord{Type: "checkpoint", Cursor: checkpoint}); err != nil {
			return fmt.Errorf("nexus: write checkpoint: %w", err)
		}
	}
}

func writeEvents(encoder *json.Encoder, next func() (nexus.Event, error)) error {
	encoder.SetEscapeHTML(false)
	for {
		event, err := next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		output := eventRecord{Type: event.Type, Artifact: event.Artifact}
		if err := encoder.Encode(output); err != nil {
			return fmt.Errorf("nexus: write event: %w", err)
		}
	}
}

func readCursor(path string) (*nexus.Cursor, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("nexus: open cursor: %w", err)
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(io.LimitReader(file, maxCursorBytes+1))
	if err != nil {
		return nil, fmt.Errorf("nexus: read cursor: %w", err)
	}
	if int64(len(data)) > maxCursorBytes {
		return nil, fmt.Errorf("nexus: cursor exceeds %d bytes", maxCursorBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var cursor nexus.Cursor
	if err := decoder.Decode(&cursor); err != nil {
		return nil, fmt.Errorf("nexus: decode cursor: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("nexus: cursor contains trailing JSON")
		}
		return nil, fmt.Errorf("nexus: decode cursor: %w", err)
	}
	return &cursor, nil
}

func cloneCounter(counter *int64) *int64 {
	if counter == nil {
		return nil
	}
	clone := *counter
	return &clone
}
