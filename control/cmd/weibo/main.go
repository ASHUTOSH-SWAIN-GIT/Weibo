// Command weibo is the control-plane CLI. The dashboard subcommand boots
// the job controller and opens its web UI — the single place to submit,
// watch, and manage jobs (Flink-style).
//
//	weibo dashboard              # start the controller and open the UI
//	weibo dashboard -no-open     # start it headless (e.g. on a server)
//	weibo dashboard -addr :9000 -image weibo-runner:dev
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
	ctrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/control/trace"
	wlog "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/log"
	teltrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
	corev1 "k8s.io/api/core/v1"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "dashboard":
		os.Exit(runDashboard(os.Args[2:]))
	case "deploy":
		os.Exit(runDeploy(os.Args[2:]))
	case "jobs":
		os.Exit(runJobs(os.Args[2:]))
	case "status":
		os.Exit(runStatus(os.Args[2:]))
	case "logs":
		os.Exit(runLogs(os.Args[2:]))
	case "cancel":
		os.Exit(runCancel(os.Args[2:]))
	case "restart":
		os.Exit(runRestart(os.Args[2:]))
	case "savepoint":
		os.Exit(runSavepoint(os.Args[2:]))
	case "delete":
		os.Exit(runDelete(os.Args[2:]))
	case "runs":
		os.Exit(runRuns(os.Args[2:]))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "weibo: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `weibo — stream-processing control plane

Usage:
  weibo dashboard [flags]              Start the controller and open the web UI
  weibo deploy [flags]                 Build, push, and submit a job manifest
  weibo jobs [flags]                   List jobs
  weibo status <job-id> [flags]        Show one job's detail and history
  weibo logs <job-id> [-tail N]        Print a job's container logs
  weibo cancel <job-id>                Gracefully stop a job
  weibo restart <job-id> [-savepoint]  Resume a job (optionally from a savepoint)
   weibo savepoint <job-id> -label N    Stop a job with a named savepoint
   weibo delete <job-id> [-delete-data]  Delete a job (optionally with its durable state)
   weibo runs <job-id>                   List a job's recorded attempts, newest first

Management commands talk to a controller over REST (env WEIBO_CONTROLLER,
default http://localhost:9000). Run "weibo <command> -h" for flags.
`)
}

func runDashboard(args []string) int {
	fs := flag.NewFlagSet("dashboard", flag.ContinueOnError)
	addr := fs.String("addr", ":9000", "API + UI listen address")
	image := fs.String("image", "weibo-runner:dev", "runner image tag")
	dbPath := fs.String("db", "./weibo-control.db", "SQLite database path")
	interval := fs.Duration("reconcile", 3*time.Second, "reconcile interval")
	historyInterval := fs.Duration("history-interval", 15*time.Second, "rolling metrics history sample interval (0 disables)")
	grafanaURL := fs.String("grafana-url", os.Getenv("WEIBO_GRAFANA_URL"), "external Grafana base URL for job/run deep links; empty disables (env WEIBO_GRAFANA_URL)")
	noOpen := fs.Bool("no-open", false, "do not open the browser")
	backendKind := fs.String("backend", "docker", "container backend: docker | kubernetes")
	namespace := fs.String("namespace", "default", "kubernetes namespace (kubernetes backend)")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path (kubernetes backend; empty = default)")
	pullSecrets := fs.String("image-pull-secrets", "", "comma-separated k8s imagePullSecret names for private registries (kubernetes backend)")
	pvcSize := fs.String("pvc-size", "1Gi", "per-job PVC size (kubernetes backend)")
	storageClass := fs.String("storage-class", "", "PVC storage class; empty = cluster default (kubernetes backend)")
	jobServiceAccount := fs.String("job-service-account", "", "service account assigned to runner pods; empty = namespace default (kubernetes backend)")
	runtimeClass := fs.String("job-runtime-class", "", "runtimeClassName assigned to runner pods (kubernetes backend)")
	priorityClass := fs.String("job-priority-class", "", "priorityClassName assigned to runner pods (kubernetes backend)")
	nodeSelector := fs.String("job-node-selector", "", "comma-separated key=value node selector for runner pods (kubernetes backend)")
	tolerations := fs.String("job-tolerations", "", "comma-separated tolerations key[=value][:effect], e.g. dedicated=weibo:NoSchedule (kubernetes backend)")
	jobTTL := fs.Int("job-ttl-after-finished", -1, "seconds Kubernetes keeps finished runner Jobs; -1 leaves cleanup to Weibo (kubernetes backend)")
	controlAddress := fs.String("k8s-control-address-template", "", "agent address template with {service}, {namespace}, {port}; empty = cluster DNS")
	maxJobs := fs.Int("max-jobs", 0, "maximum concurrent jobs; 0 = resource-limited only")
	defaultJobCPU := fs.String("default-job-cpu", "1", "default CPU reserved per job for capacity math")
	defaultJobMemory := fs.String("default-job-memory", "1Gi", "default memory reserved per job for capacity math")
	authToken := fs.String("auth-token", os.Getenv("WEIBO_AUTH_TOKEN"), "shared bearer token required by the API + UI; empty = open (env WEIBO_AUTH_TOKEN)")
	authTokenSHA256 := fs.String("auth-token-sha256", os.Getenv("WEIBO_AUTH_TOKEN_SHA256"), "comma-separated SHA-256 bearer token hashes; optional readonly:/readwrite: prefixes (env WEIBO_AUTH_TOKEN_SHA256)")
	allowOpenPublic := fs.Bool("allow-open-public", envBool("WEIBO_ALLOW_OPEN_PUBLIC"), "allow an unauthenticated public bind; required for -addr :PORT/0.0.0.0 without auth")
	logLevel := fs.String("log-level", envOr("WEIBO_LOG_LEVEL", "info"), "log level: debug|info|warn|error (env WEIBO_LOG_LEVEL)")
	logFormat := fs.String("log-format", envOr("WEIBO_LOG_FORMAT", "text"), "log format: text|json (env WEIBO_LOG_FORMAT)")
	otelEndpoint := fs.String("otel-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP/HTTP traces endpoint; empty disables tracing (env OTEL_EXPORTER_OTLP_ENDPOINT)")
	otelService := fs.String("otel-service-name", envOr("OTEL_SERVICE_NAME", "weibo-controller"), "service name for traces (env OTEL_SERVICE_NAME)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if !authConfigured(*authToken, *authTokenSHA256) && isPublicBind(*addr) && !*allowOpenPublic {
		fmt.Fprintf(os.Stderr, "weibo: refusing unauthenticated public bind on %q; set -auth-token or -auth-token-sha256, bind to localhost, or pass -allow-open-public\n", *addr)
		return 2
	}

	level, err := wlog.ParseLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: %v\n", err)
		return 2
	}
	logger := wlog.New(level, *logFormat)
	tel, shutdownTracing, err := teltrace.Configure(context.Background(), teltrace.Options{
		Endpoint: *otelEndpoint, ServiceName: *otelService,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: tracing: %v\n", err)
		return 1
	}
	if shutdownTracing != nil {
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTracing(shutCtx)
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build and preflight the selected backend — a clear message beats a
	// launch-time failure later.
	ns, err := parseKeyValues(*nodeSelector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: -job-node-selector: %v\n", err)
		return 2
	}
	tols, err := parseTolerations(*tolerations)
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: -job-tolerations: %v\n", err)
		return 2
	}

	ttl := int32PtrFromNonNegative(*jobTTL)
	be, rc := makeBackend(ctx, *backendKind, *image, *namespace, *kubeconfig, splitCSV(*pullSecrets), *pvcSize, *storageClass, *jobServiceAccount, *runtimeClass, *priorityClass, ns, tols, ttl, *controlAddress)
	if be == nil {
		return rc
	}

	st, err := store.OpenSQLite(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: open store: %v\n", err)
		return 1
	}
	defer st.Close()

	ctrl := control.New(control.Options{
		Store:   st,
		Backend: be,
		Image:   *image,
		Capacity: backend.CapacityConfig{
			MaxJobs:          *maxJobs,
			DefaultJobCPU:    *defaultJobCPU,
			DefaultJobMemory: *defaultJobMemory,
		},
		GrafanaURL: *grafanaURL,
		Logger:     logger,
		Tracer:     ctrace.Adapt(tel),
	})
	if *historyInterval > 0 {
		go ctrl.RunHistoryRecorder(ctx, *historyInterval)
	}
	// Recover labeled backend orphans left by crashed deletes/removes
	// before serving. A sweep failure is logged, never fatal: the
	// reconciler still converges live runs.
	sweepCtx, sweepCancel := context.WithTimeout(ctx, 30*time.Second)
	if rep, err := ctrl.SweepOrphans(sweepCtx); err != nil {
		logger.Warn("startup orphan sweep failed", "error", err)
	} else if len(rep.Removed) > 0 || len(rep.RunningOrphans) > 0 {
		logger.Info("startup orphan sweep",
			"removed", len(rep.Removed), "running_orphans", len(rep.RunningOrphans))
	}
	sweepCancel()
	go ctrl.RunReconciler(ctx, *interval)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewServerWithAuth(ctrl, api.AuthConfig{Token: *authToken, TokenSHA256: splitCSV(*authTokenSHA256)}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	url := browserURL(*addr)
	if !*noOpen {
		go openWhenReady(ctx, url)
	}
	logger.Info("weibo dashboard", "url", url, "image", *image, "db", *dbPath)
	if authConfigured(*authToken, *authTokenSHA256) {
		logger.Info("weibo dashboard: API auth ENABLED — clients need -token / WEIBO_TOKEN")
	} else {
		logger.Warn("weibo dashboard: API auth DISABLED; bind is local or explicitly allowed", "addr", *addr)
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "weibo: serve: %v\n", err)
		return 1
	}
	logger.Info("weibo dashboard: stopped")
	return 0
}

// makeBackend constructs and preflights the chosen container backend. On
// failure it prints a hint and returns (nil, exitCode).
func makeBackend(ctx context.Context, kind, image, namespace, kubeconfig string, pullSecrets []string, pvcSize, storageClass, jobServiceAccount, runtimeClass, priorityClass string, nodeSelector map[string]string, tolerations []corev1.Toleration, ttlSecondsAfterFinished *int32, controlAddress string) (backend.ContainerBackend, int) {
	switch kind {
	case "docker":
		d, err := backend.NewDocker(image)
		if err != nil {
			fmt.Fprintf(os.Stderr, "weibo: docker: %v\n", err)
			return nil, 1
		}
		if err := d.Ping(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "weibo: Docker daemon not reachable — is Docker running?")
			return nil, 1
		}
		if ok, err := d.HasImage(ctx, image); err == nil && !ok {
			// image is only the default runner for plain YAML workflow jobs;
			// SDK jobs bring their own image and never touch it. Missing it
			// is not fatal — it would only refuse the controller to start on
			// a host that will only ever run SDK jobs — so warn and continue;
			// a YAML job submitted later fails with the same message at
			// launch time instead.
			fmt.Fprintf(os.Stderr, "weibo: runner image %q not found; plain YAML workflow jobs will fail to launch until it is built:\n"+
				"  docker build -f Dockerfile.runner -t %s .\n", image, image)
		}
		return d, 0
	case "kubernetes", "k8s":
		kb, err := backend.NewKubernetes(backend.KubernetesOptions{
			Kubeconfig: kubeconfig, Namespace: namespace, Image: image,
			ImagePullSecrets: pullSecrets,
			PVCSize:          pvcSize, StorageClass: storageClass,
			ServiceAccountName:      jobServiceAccount,
			RuntimeClassName:        runtimeClass,
			PriorityClassName:       priorityClass,
			NodeSelector:            nodeSelector,
			Tolerations:             tolerations,
			TTLSecondsAfterFinished: ttlSecondsAfterFinished,
			ControlAddressTemplate:  controlAddress,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "weibo: kubernetes: %v\n", err)
			return nil, 1
		}
		if err := kb.Ping(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "weibo: kubernetes API not reachable: %v\n"+
				"  check your kubeconfig / cluster, and that the image %q is pushed to a registry the cluster can pull.\n", err, image)
			return nil, 1
		}
		return kb, 0
	default:
		fmt.Fprintf(os.Stderr, "weibo: unknown backend %q (want docker or kubernetes)\n", kind)
		return nil, 2
	}
}

// splitCSV parses a comma-separated flag value into a trimmed, non-empty
// slice (nil when empty).
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func authConfigured(token, hashes string) bool {
	return strings.TrimSpace(token) != "" || len(splitCSV(hashes)) > 0
}

func isPublicBind(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.HasPrefix(addr, ":") {
			return true
		}
		return false
	}
	host = strings.Trim(host, "[]")
	return host == "" || host == "0.0.0.0" || host == "::"
}

func parseKeyValues(s string) (map[string]string, error) {
	parts := splitCSV(s)
	if len(parts) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(parts))
	for _, part := range parts {
		key, val, ok := strings.Cut(part, "=")
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("expected key=value, got %q", part)
		}
		out[key] = val
	}
	return out, nil
}

func parseTolerations(s string) ([]corev1.Toleration, error) {
	parts := splitCSV(s)
	if len(parts) == 0 {
		return nil, nil
	}
	out := make([]corev1.Toleration, 0, len(parts))
	for _, part := range parts {
		body, effectText, _ := strings.Cut(part, ":")
		key, value, hasValue := strings.Cut(body, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		effectText = strings.TrimSpace(effectText)
		if key == "" {
			return nil, fmt.Errorf("empty toleration key in %q", part)
		}
		tol := corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists}
		if hasValue {
			if value == "" {
				return nil, fmt.Errorf("empty toleration value in %q", part)
			}
			tol.Operator = corev1.TolerationOpEqual
			tol.Value = value
		}
		if effectText != "" {
			effect := corev1.TaintEffect(effectText)
			switch effect {
			case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
				tol.Effect = effect
			default:
				return nil, fmt.Errorf("invalid toleration effect %q in %q", effectText, part)
			}
		}
		out = append(out, tol)
	}
	return out, nil
}

func int32PtrFromNonNegative(n int) *int32 {
	if n < 0 {
		return nil
	}
	v := int32(n)
	return &v
}

// browserURL turns a listen address (":9000", "0.0.0.0:9000") into a URL
// a browser on this machine can open.
func browserURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://localhost" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

// openWhenReady waits until the server answers, then opens the browser.
func openWhenReady(ctx context.Context, url string) {
	deadline := time.Now().Add(8 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		if resp, err := client.Get(url + "/healthz"); err == nil {
			resp.Body.Close()
			openBrowser(url)
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// openBrowser opens url in the default browser (best effort).
func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
		args = []string{url}
	case "windows":
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler", url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}
	_ = exec.Command(cmd, args...).Start()
}
