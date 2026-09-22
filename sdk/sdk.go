// Package sdk is the harness for SDK (Go) jobs run through the weibo
// control plane. A user writes a pipeline builder and calls Run:
//
//	func main() { sdk.Run(Build) }
//	func Build(env *weibo.StreamExecutionEnv) {
//	    env.FromSource(src).KeyBy(key).Reduce(sum).ToSink(out)
//	}
//
// The harness supervises the pipeline exactly like the YAML runner: it
// serves the control surface (/state, /metrics, /cancel, /savepoint) for
// the dashboard, drains gracefully on SIGTERM, and — when checkpointing is
// enabled (CHECKPOINT_INTERVAL) — supports savepoints and recovery. So an
// SDK job is managed identically to a YAML job.
//
// Configuration (environment):
//
//	DATA_DIR             state/checkpoint root                (default /data)
//	PORT                 control port                          (default 8080)
//	SAVEPOINT_DIR        savepoint blobstore                   (default /savepoints)
//	SAVEPOINT_S3_BUCKET  use S3-compatible savepoint storage   (default off)
//	CHECKPOINT_INTERVAL  enable durable checkpointing, e.g. 5s (default off)
//	CHECKPOINT_RETENTION completed recovery points to keep          (default 3)
//	RESTORE_SAVEPOINT    savepoint label to resume from        (default none)
//	JOB_NAME             human-readable name for logs
//
// weibo-runner additionally honors LOG_LEVEL, LOG_FORMAT,
// OTEL_EXPORTER_OTLP_ENDPOINT, and OTEL_SERVICE_NAME for its own
// logger/tracer; plain SDK mains pass Logger/Tracer via ServeOptions.
package sdk

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultDataDir      = "/data"
	defaultPort         = "8080"
	defaultSavepointDir = "/savepoints"
)

// Builder wires a pipeline onto env: env.FromSource(...)....ToSink(...).
type Builder func(env *weibo.StreamExecutionEnv)

// Run is the SDK job entrypoint. It configures the environment from env
// vars, invokes build to wire the pipeline, and supervises it to
// completion, exiting the process with an appropriate status code.
func Run(build Builder) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runBuild(ctx, build, os.Getenv, os.Stdout, os.Stderr))
}

// runBuild is Run's testable core.
func runBuild(ctx context.Context, build Builder, getenv func(string) string, stdout, stderr io.Writer) int {
	dataDir := orDefault(getenv("DATA_DIR"), defaultDataDir)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "sdk: create data dir %s: %v\n", dataDir, err)
		return 1
	}

	env := weibo.NewEnv()
	checkpointDir := ""
	// Opt-in durable checkpointing: the harness owns the storage layout so
	// savepoints/recovery work identically to YAML jobs.
	if iv := getenv("CHECKPOINT_INTERVAL"); iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil || d <= 0 {
			fmt.Fprintf(stderr, "sdk: invalid CHECKPOINT_INTERVAL %q\n", iv)
			return 2
		}
		checkpointDir = filepath.Join(dataDir, "checkpoints")
		stateDir := filepath.Join(dataDir, "state")
		if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
			fmt.Fprintf(stderr, "sdk: create checkpoint dir: %v\n", err)
			return 1
		}
		if err := os.MkdirAll(stateDir, 0o755); err != nil {
			fmt.Fprintf(stderr, "sdk: create state dir: %v\n", err)
			return 1
		}
		retain := checkpoint.DefaultRetainedCheckpoints
		if value := getenv("CHECKPOINT_RETENTION"); value != "" {
			parsed, parseErr := strconv.Atoi(value)
			if parseErr != nil || parsed < 1 {
				fmt.Fprintf(stderr, "sdk: invalid CHECKPOINT_RETENTION %q\n", value)
				return 2
			}
			retain = parsed
		}
		storage, err := checkpoint.NewFileStorageWithOptions(checkpointDir, checkpoint.FileStorageOptions{RetainCompleted: retain})
		if err != nil {
			fmt.Fprintf(stderr, "sdk: initialize checkpoint storage: %v\n", err)
			return 1
		}
		env.WithCheckpointing(d, storage)
		env.WithStateBackend(state.Pebble(stateDir))
	}

	build(env)

	return Serve(ctx, env, ServeOptions{
		Name:             orDefault(getenv("JOB_NAME"), "sdk-job"),
		Port:             getenv("PORT"),
		CheckpointDir:    checkpointDir,
		SavepointDir:     getenv("SAVEPOINT_DIR"),
		SavepointS3:      SavepointS3FromEnv(getenv),
		RestoreSavepoint: getenv("RESTORE_SAVEPOINT"),
		Stdout:           stdout,
		Stderr:           stderr,
	})
}

// ServeOptions configures Serve.
type ServeOptions struct {
	Name             string // for log lines
	Port             string // control port; default 8080
	CheckpointDir    string // "" when checkpointing is disabled
	SavepointDir     string // savepoint blobstore; default /savepoints
	SavepointS3      SavepointS3Options
	RestoreSavepoint string // savepoint label to seed from, or ""
	Stdout, Stderr   io.Writer
	// Logger sets structured logging for the engine and agent (nil keeps
	// their defaults). Tracer enables the agent's job-run span and
	// engine checkpoint spans (nil is no-op).
	Logger *slog.Logger
	Tracer trace.Tracer
}

type SavepointS3Options struct {
	Bucket       string
	Prefix       string
	Region       string
	Endpoint     string
	PathStyle    bool
	SSE          string
	KMSKeyID     string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func SavepointS3FromEnv(getenv func(string) string) SavepointS3Options {
	return SavepointS3Options{
		Bucket:       getenv("SAVEPOINT_S3_BUCKET"),
		Prefix:       getenv("SAVEPOINT_S3_PREFIX"),
		Region:       getenv("AWS_REGION"),
		Endpoint:     getenv("SAVEPOINT_S3_ENDPOINT"),
		PathStyle:    truthy(getenv("SAVEPOINT_S3_PATH_STYLE")),
		SSE:          getenv("SAVEPOINT_S3_SSE"),
		KMSKeyID:     getenv("SAVEPOINT_S3_KMS_KEY_ID"),
		AccessKey:    getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:    getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: getenv("AWS_SESSION_TOKEN"),
	}
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

// Serve supervises a configured env under a jobagent: it restores a
// savepoint if asked, serves the control surface, runs to completion, and
// promotes a savepoint on a stop-with-savepoint request. It returns a
// process exit code. This is the shared lifecycle behind both the YAML
// runner and SDK jobs, so they behave identically.
func Serve(ctx context.Context, env *weibo.StreamExecutionEnv, opts ServeOptions) int {
	stdout := orWriter(opts.Stdout, os.Stdout)
	stderr := orWriter(opts.Stderr, os.Stderr)
	port := orDefault(opts.Port, defaultPort)
	blobs, err := savepointBlobstore(opts)
	if err != nil {
		fmt.Fprintf(stderr, "sdk: savepoint blobstore: %v\n", err)
		return 1
	}

	if opts.RestoreSavepoint != "" {
		if opts.CheckpointDir == "" {
			fmt.Fprintln(stderr, "sdk: RESTORE_SAVEPOINT set but checkpointing is disabled")
			return 1
		}
		id, err := checkpoint.RestoreSavepoint(checkpoint.NewFileStorage(opts.CheckpointDir), blobs, opts.RestoreSavepoint)
		if err != nil {
			fmt.Fprintf(stderr, "sdk: restore savepoint %q: %v\n", opts.RestoreSavepoint, err)
			return 1
		}
		fmt.Fprintf(stdout, "sdk: restored from savepoint %q (checkpoint %s)\n", opts.RestoreSavepoint, id)
	}

	if opts.Logger != nil {
		env.WithLogger(opts.Logger)
	}
	if opts.Tracer != nil {
		env.WithTracer(opts.Tracer)
	}
	agent := jobagent.New(env)
	if opts.Logger != nil {
		agent.SetLogger(opts.Logger)
	}
	if opts.Tracer != nil {
		agent.SetTracer(opts.Tracer)
	}

	// Serve the control surface alongside the job; its own context tears
	// it down when the job finishes on its own, not only on SIGTERM.
	srvCtx, stopSrv := context.WithCancel(ctx)
	defer stopSrv()
	serveErr := make(chan error, 1)
	go func() { serveErr <- agent.Serve(srvCtx, ":"+port) }()

	runErr := agent.Run(ctx)
	stopSrv()

	// Stop-with-savepoint: promote the final checkpoint now.
	if label, ok := agent.SavepointRequest(); ok {
		if opts.CheckpointDir == "" {
			fmt.Fprintln(stderr, "sdk: savepoint requested but checkpointing is disabled")
		} else if id, err := checkpoint.CreateSavepoint(checkpoint.NewFileStorage(opts.CheckpointDir), blobs, label); err != nil {
			fmt.Fprintf(stderr, "sdk: create savepoint %q: %v\n", label, err)
		} else {
			fmt.Fprintf(stdout, "sdk: savepoint %q created from checkpoint %s\n", label, id)
		}
	}

	if err := <-serveErr; err != nil {
		fmt.Fprintf(stderr, "sdk: control server: %v\n", err)
	}

	// Defer to agent.Run's own Phase decision rather than re-deriving
	// "was this really a failure" from runErr here too: re-checking
	// errors.Is(runErr, context.Canceled) independently repeats the exact
	// bug found live in Execute/Run — a context.Canceled error can come
	// from an unrequested internal force-unwind (e.g. a dead Kafka
	// broker), not just a real shutdown request, and agent.Run is the
	// one place with enough context (whether shutdown was actually
	// requested) to tell those apart correctly.
	switch agent.State().Phase {
	case jobagent.PhaseFailed:
		fmt.Fprintf(stderr, "sdk: job=%s failed: %v\n", opts.Name, runErr)
		return 1
	default:
		fmt.Fprintf(stdout, "sdk: job=%s %s\n", opts.Name, agent.State().Phase)
		return 0
	}
}

func savepointBlobstore(opts ServeOptions) (checkpoint.Blobstore, error) {
	if opts.SavepointS3.Bucket == "" {
		return checkpoint.NewFileBlobstore(orDefault(opts.SavepointDir, defaultSavepointDir)), nil
	}
	s3opts := []checkpoint.S3BlobstoreOption{
		checkpoint.S3BlobBucket(opts.SavepointS3.Bucket),
		checkpoint.S3BlobPrefix(opts.SavepointS3.Prefix),
		checkpoint.S3BlobRegion(opts.SavepointS3.Region),
		checkpoint.S3BlobEndpoint(opts.SavepointS3.Endpoint),
		checkpoint.S3BlobStaticCredentials(opts.SavepointS3.AccessKey, opts.SavepointS3.SecretKey, opts.SavepointS3.SessionToken),
	}
	if opts.SavepointS3.PathStyle {
		s3opts = append(s3opts, checkpoint.S3BlobPathStyle())
	}
	if opts.SavepointS3.SSE != "" {
		s3opts = append(s3opts, checkpoint.S3BlobSSE(types.ServerSideEncryption(opts.SavepointS3.SSE), opts.SavepointS3.KMSKeyID))
	}
	return checkpoint.NewS3Blobstore(s3opts...)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orWriter(w, def io.Writer) io.Writer {
	if w == nil {
		return def
	}
	return w
}
