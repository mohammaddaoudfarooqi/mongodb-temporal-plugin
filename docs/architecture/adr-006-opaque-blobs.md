# ADR-006: Opaque blobs via BSON Binary + encoding field

- Status: Accepted
- Date: 2026-04-30
- Deciders: plugin architecture review

## Context

Temporal persists many values as opaque protobuf or JSON payloads:
execution info, execution state, activity/timer/signal infos,
history events, visibility memos. The upstream contract is to hand
these to the store as `commonpb.DataBlob` and to return them
unchanged.

If the plugin deserialized the protobuf to enrich queries, it
would:

1. Couple tightly to Temporal's protobuf versions.
2. Create IP risk — upstream .proto semantics bleed into our code.
3. Force a version bump every time upstream adds a field.

## Decision

Store blobs as a two-field subdocument:

```
{ data: <BinData>, encoding: "proto3" | "json" }
```

The plugin never deserializes `data`. Indexed fields
(`next_event_id`, `db_record_version`, `run_id`, `status`, `state`,
`start_time`) are stored adjacent to the blob as first-class BSON
fields — they are the only things the plugin queries by.

## Alternatives considered

- **Deserialize and project** — rejected: breaks upstream proto
  ownership; doubles storage (projected fields + payload).
- **Flatten into BSON** — rejected: invasive, proto-version-coupled,
  and unnecessary given the indexed-adjacent pattern.

## Consequences

- Clean IP story — the plugin transports opaque bytes; upstream
  defines the schema.
- Size budgeting is straightforward: sum of blob
  lengths + a small constant overhead.
- Version-mixing across plugin releases is safe — adding a new
  upstream proto field is transparent to us.
