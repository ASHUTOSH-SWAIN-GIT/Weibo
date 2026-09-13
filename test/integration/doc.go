// Package integration holds weibo's integration tiers (roadmap #26).
//
// Every tier has two halves:
//
//   - Offline (fake) subtests run on every pull request with no external
//     services: httptest servers, FileBlobstore/FileStorage savepoints,
//     SASL/TLS config validation, checkpoint-envelope invariants, and
//     broker-loss error propagation against closed ports.
//
//   - Live subtests are skipped unless the backend is provided by the
//     environment (KAFKA_BROKERS, POSTGRES_DSN, SAVEPOINT_S3_BUCKET,
//     WEIBO_RUN_KIND=1). Hosted CI runs the offline half on every PR
//     (.github/workflows/ci.yml) and the live halves on merge/nightly
//     (.github/workflows/integration.yml), where services supply Kafka,
//     Postgres, MinIO (S3), and kind.
//
// Naming: TestTier_* is offline; TestLive_* needs a real backend.
package integration_test
