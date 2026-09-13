# Checkpoint schema and public API compatibility

This document records the compatibility expectations for checkpoint files,
savepoints, and control-plane APIs.

## Checkpoints

`checkpoint.CheckpointData` is the durable recovery envelope written by
`checkpoint.FileStorage`.

Top-level fields:

- `id` — checkpoint identifier.
- `timestamp` — checkpoint completion/preparation time.
- `operators` — inline operator snapshots keyed by stable owner IDs.
- `source` — source-specific recovery blobs. The standard source-offset key is
  `offset`.
- `stateDirs` — native state-backend checkpoint directories keyed by owner ID.
- `status` — `completed` for usable recovery points, `prepared` for a
  transactional checkpoint whose sink transaction outcome must be resolved.
- `txnId` — transactional sink identity when coordinated checkpoints are used.
- `error` — optional diagnostic text for failed/prepared paths.

Source offsets use `source.EncodePositions` version `2`:

```json
{
  "version": 2,
  "positions": [
    {"source": "topic-a", "partition": 0, "offset": 42}
  ]
}
```

Legacy single-topic partition maps (`{"0": 42}`) remain readable only when the
source can infer one unambiguous topic/source. Ambiguous legacy multi-topic
restores fail explicitly instead of guessing.

## Savepoints

A savepoint is a named promotion of checkpoint blobs under
`savepoints/<label>`. It is portable only within the storage namespace that can
read those blobs:

- Docker currently uses a shared local volume for savepoints.
- Kubernetes currently stores same-job savepoints under the job PVC.
- Cross-job/cross-cluster portability requires future object-store-backed
  checkpoint/blob storage.

## Durable-state compatibility rules

- Completed checkpoints and savepoints from supported prior formats should keep
  loading.
- New checkpoint schemas must be versioned when interpretation changes.
- Prepared transactional checkpoints must never be garbage-collected until their
  sink transaction outcome is resolved or explicitly discarded by recovery.
- Unknown or ambiguous durable state should stop startup with a clear error; it
  must not silently skip data or duplicate committed output.

## Public API compatibility

The control-plane REST API is source-compatible within the current unversioned
surface:

- Existing routes and JSON fields should remain valid.
- New response fields may be added; clients should ignore unknown fields.
- Routes that mutate jobs require bearer-token auth when the controller is
  configured with `WEIBO_AUTH_TOKEN`.
- `GET /`, `GET /healthz`, `GET /livez`, `GET /readyz`, and `GET /metrics`
  remain unauthenticated operational endpoints.
- `/livez` reports process liveness. `/readyz` reports dependency readiness and
  may return `503` while the process remains live.

Breaking API changes should either introduce a versioned route or be called out
in release notes with a migration path.
