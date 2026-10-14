# Versioning

How this repo uses semver across its three distinct surfaces.

## Three surfaces, three compatibility contracts

| Surface                  | Versioned by         | Breakage signal                      |
|--------------------------|----------------------|--------------------------------------|
| `pkg/plugin/` Go API     | Go module semver tag | Minor import change → minor bump; signature change → major |
| YAML config schema       | Config `version` key | Removed key → major; renamed key → major with alias for one minor |
| On-disk MongoDB schema   | `_schema_version`    | Data shape change → schema `major.minor` bump + migration |

All three advance together at GA (`v1.0.0`) but can bump
independently afterwards.

## Go module semver (`pkg/plugin/`)

- **MAJOR** — breaking change to `Config`, `NewFactory`,
  `NewVisibilityFactory`, or any exported type a consumer binary
  imports. Requires a new major tag (`v2.0.0`) and a new module
  path per Go module rules.
- **MINOR** — new exported symbols, new config keys (additive,
  backwards compatible). Unknown options are tolerated with a
  warning per `docs/operator/config-reference.md`.
- **PATCH** — bug fixes, doc updates, internal refactors.

Internal packages (`internal/...`) have no stability guarantee.
Consumers MUST NOT depend on them.

## YAML config schema

Forward-compatibility is a first-class goal: a config written for
plugin v1.2 should still boot on v1.1 with a warning if it mentions
a v1.2-only key.

- Add a new option → MINOR bump.
- Remove a required option → MAJOR bump.
- Rename an option → MAJOR bump, with the old name accepted for
  one MINOR cycle with a deprecation warning.

Keep `docs/operator/config-reference.md` in lockstep with any
schema change.

## On-disk schema

`_schema_version` holds `{major: N, minor: M}`. Semantics:

- Plugin refuses to start if `_schema_version.major` differs from
  the plugin's compiled-in major. Forces explicit migration.
- Plugin starts if `_schema_version.minor` < compiled minor,
  logs a warning, and recommends running `update-schema`.
- Plugin refuses to start if `_schema_version.minor` > compiled
  minor — prevents silent downgrade on top of newer data.
- `numHistoryShards` must equal the value stored at setup time;
  mismatch is a hard startup failure.

Migrations are additive (new collections, new indexes). Destructive
migrations (drop collection, rename field) trigger a MAJOR bump,
are announced one minor version in advance, and ship a migration
script.

## Compatibility matrix

`docs/operator/sizing.md` has the authoritative support matrix. At
GA it will read roughly:

| Plugin | Temporal server    | MongoDB            | Go (build) |
|--------|--------------------|--------------------|------------|
| 1.0.x  | last 2 minor       | 6.0, 7.0, 8.0      | per go.mod |

The `last 2 minor` clause lets the plugin track upstream without
every Temporal release forcing a new tag.

## Deprecation policy

- Deprecations are announced in a MINOR release; removal lands in
  the next MAJOR.
- Mark deprecated symbols with `// Deprecated: ...` and a pointer
  to the replacement.
- Changelog calls out deprecations under a dedicated heading so
  operators see them during upgrade planning.

## Release cadence

- MINOR: as features land, nominally every 4–8 weeks.
- PATCH: on demand for bug fixes or CVE responses.
- MAJOR: only when required by upstream breakage or a schema
  redesign; planned, not reactive.

See `docs/developer/release.md` for the mechanical release steps.
