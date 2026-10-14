# Supply chain

How we keep the build and release path trustworthy.

## Reproducible builds

- `make build` is deterministic given a fixed Go toolchain, module
  cache, and `GOFLAGS="-trimpath -buildvcs=false"`. The release
  job pins Go via `actions/setup-go` and caches modules keyed on
  `go.sum`.
- The binary embeds its version, commit SHA, and build date via
  `internal/buildinfo` using `-ldflags`. All three are printed by
  `temporal-mongodb-tool version`.
- Rebuilding from the same tagged source must produce byte-identical
  binaries for the same OS/arch pair. If it does not, the build job
  fails and the release is blocked.

## Module integrity

- `go mod verify` runs on every CI job; any mismatch between
  `go.sum` and the module cache fails the build.
- Renovate (or Dependabot) is configured to open PRs for
  dependency bumps. Bumps that affect license metadata also
  require a `NOTICE` update in the same PR (CI enforces the
  three-file invariant: `go.mod`, `go.sum`, `NOTICE`).
- No `replace` directives in `go.mod` at release time. Local
  development is free to add them; CI rejects PRs that retain a
  `replace` when targeting `main`.

## License hygiene

- Direct dependencies must be Apache 2.0 / MIT / BSD / ISC. GPL
  and LGPL are incompatible with the plugin's Apache 2.0 license
  and will be rejected at review.
- `make license-check` scans `go.sum` with `go-licenses` (or the
  equivalent tool) and fails on any non-allow-listed license.
- `NOTICE` carries attribution for every non-trivial direct
  dependency.

## Signing

Release artifacts are signed with
[cosign](https://docs.sigstore.dev/cosign):

- Keyless signing (via Sigstore's OIDC + Fulcio) is preferred. The
  GitHub Actions job has `id-token: write` for this purpose.
- `SHA256SUMS.sig` accompanies `SHA256SUMS` in every release.
- Operators verify with:
  ```
  cosign verify-blob \
    --certificate-identity-regexp 'https://github\.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/\.github/workflows/release\.yml@.*' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    --signature SHA256SUMS.sig \
    SHA256SUMS
  ```

Git tags are signed with the release manager's GPG key. The public
key fingerprint is published in the security page of the repo.

## Container images (if shipped)

- Built from `gcr.io/distroless/static:nonroot`, no shell, no
  package manager.
- Multi-arch manifest (`linux/amd64`, `linux/arm64`).
- Image is signed with cosign; SBOM (SPDX) attached via `cosign
  attest --predicate`.
- The Dockerfile lives next to the binary it ships; no external
  base-image assembly.

## SBOM

`make sbom` produces `sbom.spdx.json` using
`cyclonedx-gomod` or `syft`. It is attached to the GitHub release.

Operators can audit the SBOM against their vulnerability scanner
(Trivy, Grype, Snyk) before deploying.

## Vulnerability response

- `govulncheck ./...` runs on every CI job. A new finding fails the
  build.
- CVE response target: triage within 24 hours, patch release
  within 7 days for HIGH/CRITICAL in the plugin or a direct
  dependency on the runtime path.
- `SECURITY.md` documents how to report vulnerabilities privately
  (GitHub security advisories or email).

## What is intentionally out of scope for v1.0

- In-toto attestations beyond the SBOM. (Candidate for v1.1.)
- Reproducible-build verification by third parties via `rebuilderd`.
  (Candidate for post-GA.)
