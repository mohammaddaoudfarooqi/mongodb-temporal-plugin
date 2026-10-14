# Security Policy

## Reporting vulnerabilities

If you discover a security vulnerability in `mongodb-temporal-plugin`, please
**do not** open a public GitHub issue. Instead, report it privately through
GitHub's private vulnerability reporting on this repository (Security →
Report a vulnerability).

Include:

- A description of the vulnerability and its impact.
- Steps to reproduce, including minimal code and configuration.
- The plugin version, MongoDB server version, and Temporal server version.
- Any suggested mitigations.

## Response

This is a reference implementation maintained on a best-effort basis (see
the README). Reports are acknowledged and triaged as quickly as we can;
fixes go into the latest release only.

## Scope

In scope:

- The plugin Go code under `pkg/plugin/` and `internal/`.
- The schema tool under `cmd/temporal-mongodb-tool/`.
- The reference server under `cmd/temporal-mongodb-server/`.
- The quickstarts and deployment scripts under `quickstart/` and `deploy/`.
- Schema manifests under `schema/`.

Out of scope (report upstream):

- Vulnerabilities in Temporal server itself → `temporalio/temporal` security
  policy.
- Vulnerabilities in MongoDB server or the Go driver → MongoDB's product
  security team.
- Vulnerabilities in third-party dependencies → upstream project; we will
  respond by updating the dependency.
