# nexus

`nexus` reads Maven repository indexes without Java or Lucene. It downloads the index properties first, selects a full index or the required incremental chunks, and streams artifact additions and removals in publication order.

It is intended for catalog importers, cache warmers, and Maven proxy services that need registry metadata without crawling artifact paths or fetching every POM. Repository discovery, storage, scheduling, and POM enrichment stay with the calling program.

## Library use

Create a client with an optional cursor from the last completed synchronization:

```go
client := nexus.NewClient(nexus.ClientOptions{})

sync, err := client.Sync(ctx, repositoryURL, cursor)
if err != nil {
    return err
}
defer sync.Close()

if sync.Mode() == nexus.SyncFull {
    // Replace existing catalog state when this sync commits.
}

for {
    chunk, err := sync.NextChunk()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }

    for {
        event, err := chunk.Next()
        if errors.Is(err, io.EOF) {
            break
        }
        if err != nil {
            return err
        }
        // Apply event.Type and event.Artifact in order.
    }

    checkpoint, err := chunk.Checkpoint()
    if err != nil {
        return err
    }
    // Commit the chunk's events and checkpoint together.
    cursor = &checkpoint
}
```

`Checkpoint` succeeds only after the chunk reaches a clean gzip EOF and passes its checksum. Chunk timestamps must not predate the prior cursor or exceed the advertised publication time. Before the final checkpoint, the client conditionally fetches the index properties again and returns `ErrSyncPlanChanged` if the selected plan has changed. An early close, truncated response, malformed record, or inconsistent publication leaves the cursor unchanged. Full synchronization means replacement rather than applying a snapshot over old catalog state.

If an advertised chunk disappears, the client refreshes the properties once. `NextChunk` returns `ErrSyncPlanChanged` if that refresh changes the mode, target, or remaining chunks. Start a new synchronization with the latest committed checkpoint in that case.

The lower-level `NewReader` and `NewRawReader` functions parse saved chunks or caller-managed streams without HTTP.

## Command

Build the command locally:

```console
go build -o nexus ./cmd/nexus
```

Parse a saved chunk or standard input:

```console
nexus parse nexus-maven-repository-index.42.gz
nexus parse - < nexus-maven-repository-index.gz
```

Synchronize a repository from a cold start or a saved cursor:

```console
nexus sync https://repo.example.test/repository/releases/
nexus sync --cursor cursor.json https://repo.example.test/repository/releases/
```

The command writes newline-delimited JSON. A sync begins with its mode, followed by artifact events and one checkpoint after each verified chunk. The cursor file is read only; the consumer decides when and where to persist checkpoint records.

Private, loopback, CGNAT, and NAT64 addresses are refused by default. The public-address policy bypasses environment proxies and rejects an explicit proxy, custom `RoundTripper`, or custom dialer because those paths cannot be checked at connection time. Set `AllowPrivateAddresses` when using one of those transports and enforce its address policy separately. The command exposes the same opt-out as `--allow-private`.

The default transport applies 30-second connection and response-header timeouts. Body reads also have a 30-second idle timeout, configurable with `ClientOptions.ResponseIdleTimeout`. Parser limits, retry counts, and locally generated backoff bounds are available through the same options type. A valid server `Retry-After` delay is honored without shortening it.

## Use with other git-pkgs packages

`github.com/git-pkgs/pom` accepts the group ID, artifact ID, and version emitted by Nexus. A host program can use it to fetch and resolve POMs, parent models, and imported BOMs. Deduplicate by GAV before enrichment because one Maven index may contain several extensions and classifiers for the same version.

`github.com/git-pkgs/proxy` is a natural host for index-driven cache warming. Nexus can supply full and incremental artifact identities while proxy keeps ownership of storage, workers, rate limits, and serving Maven requests. A complete mirror needs the classifier and extension fields as well as GAV; POM resolution is optional unless the mirror follows dependency relationships.

## Tests and benchmarks

Run the test suite and in-process benchmarks with:

```console
go test ./...
go test -run '^$' -bench . -benchmem ./...
```

Compiled-process benchmarks are opt-in and use a local saved fixture:

```console
NEXUS_BENCHMARK_CHUNK=/path/to/nexus-maven-repository-index.gz \
  go test ./cmd/nexus -run '^$' -bench 'Benchmark(Parse|IncrementalSync)Process$'
```

The process benchmark reports startup-inclusive time, input throughput, executable size, the fixture SHA-256, Go version, operating system, architecture, and `GOMAXPROCS`.
