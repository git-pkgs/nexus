# Maven Indexer compatibility fixtures

The three gzip chunks come from `tests/repository_test` in
`ecosyste-ms/maven-index-exporter` at commit
`8c6f6c19f2455e0c92c7ec1567c7aa6b56abaf81`. That repository generated them
with Maven Indexer CLI 6.0.0 from the small test repository described in its
`docs/build_and_test.md` file.

Expected headers and records were checked with Apache Maven Indexer's
`ChunkReader` at commit `d1142b56506db94a8e5a830c75130613e39751a0`.

SHA-256:

- `nexus-maven-repository-index.gz`: `7d01fadfed966e745dfc47d74a9b5c5996fc07c3e44979a36d87df46bac9d4a8`
- `nexus-maven-repository-index.1.gz`: `dab61317015df60f5e63d4486d3dc0d65f3c42482957fd92b4b0f83b383343bd`
- `nexus-maven-repository-index.2.gz`: `21367476109b2640eb15c0c9ccab8252ebf8a601425c67e81e5e64f8d1053da6`

`THIRD_PARTY_LICENSES.txt` is copied from the source repository and contains
the notice for the test artifacts used to generate these indexes.
