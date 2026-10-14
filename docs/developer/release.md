# Release

Mechanical checklist for cutting a release. For policy around when
to bump which version component, see `versioning.md`.

## Pre-release gate

All of the following must be green before tagging:

1. `make vet staticcheck unit integration conformance dco`
2. The Atlas end-to-end suite (`ci/scripts/atlas-e2e.sh`) against a
   fresh Atlas M30 project (live Atlas Search regression).
3. Load testing meets the targets in `docs/operator/sizing.md`.
4. CHANGELOG.md updated with every user-visible change since the
   previous tag.
5. For a GA release (`v1.0.0+`), `NOTICE` is reviewed and lists
   every bundled dependency's license attribution.

## Build matrix

Release binaries are produced for:

- `linux/amd64`
- `linux/arm64`
- `darwin/amd64`
- `darwin/arm64`

Windows is not supported for v1.0. The consumer binary repo mirrors
this matrix.

A release job (not yet automated in this repository) cross-compiles
all four targets and produces:

- `temporal-mongodb-server-<version>-<os>-<arch>.tar.gz`
- `temporal-mongodb-tool-<version>-<os>-<arch>.tar.gz`
- `SHA256SUMS` + `SHA256SUMS.sig` (cosign-signed; see
  `supply-chain.md`).

## Tag and push

```bash
git tag -s v1.X.Y -m "v1.X.Y"
git push origin v1.X.Y
```

`-s` signs the tag with the maintainer's GPG key. Unsigned tags
are rejected by the release workflow.

## Module proxy

The Go module is published implicitly by the tag: `go get
github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin@v1.X.Y` works
as soon as the proxy picks up the tag (usually <5 minutes).

Test it:

```bash
GOPROXY=https://proxy.golang.org go get \
  github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin@v1.X.Y
```

## GitHub release

The release workflow:

1. Builds the four-arch matrix.
2. Uploads the tarballs + `SHA256SUMS`.
3. Generates release notes from the CHANGELOG section for this
   tag.

Manual post-release tasks:

- Update `docs/operator/sizing.md` compatibility matrix if this
  release adds/drops a MongoDB version.
- If the YAML schema changed, update the Helm chart repo (separate
  project) and cut a matching Helm chart release.

## Post-release

- Bump `main` to the next `-dev` placeholder version in
  `internal/buildinfo/version.go`.
- Triage issues filed against the previous release; label
  `regression` for anything that used to work.
- Monitor `persistence_mongo_errors_total` on the flagship Atlas
  canary for 72 hours. Roll back the recommended tag in docs if a
  regression surfaces.

## Rolling back

Tags are immutable. A rollback means:

1. Mark the bad tag as "do not use" in the CHANGELOG.
2. Cut a new patch release reverting the offending commits.
3. Bump the recommended tag in `docs/operator/deployment.md` to the
   patched version.
4. File a post-mortem and add a regression test for the root cause so
   it does not recur.
