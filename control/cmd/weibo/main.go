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
	"log"
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
	noOpen := fs.Bool("no-open", false, "do not open the browser")
	backendKind := fs.String("backend", "docker", "container backend: docker | kubernetes")
	namespace := fs.String("namespace", "default", "kubernetes namespace (kubernetes backend)")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path (kubernetes backend; empty = default)")
	pullSecrets := fs.String("image-pull-secrets", "", "comma-separated k8s imagePullSecret names for private registries (kubernetes backend)")
	pvcSize := fs.String("pvc-size", "1Gi", "per-job PVC size (kubernetes backend)")
	storageClass := fs.String("storage-class", "", "PVC storage class; empty = cluster default (kubernetes backend)")
	authToken := fs.String("auth-token", os.Getenv("WEIBO_AUTH_TOKEN"), "shared bearer token required by the API + UI; empty = open (env WEIBO_AUTH_TOKEN)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build and preflight the selected backend — a clear message beats a
	// launch-time failure later.
	be, rc := makeBackend(ctx, *backendKind, *image, *namespace, *kubeconfig, splitCSV(*pullSecrets), *pvcSize, *storageClass)
	if be == nil {
		return rc
	}

	st, err := store.OpenSQLite(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "weibo: open store: %v\n", err)
		return 1
	}
	defer st.Close()

	ctrl := control.New(control.Options{Store: st, Backend: be, Image: *image, Logf: log.Printf})
	go ctrl.RunReconciler(ctx, *interval)

	srv := &http.Server{Addr: *addr, Handler: api.NewServer(ctrl, *authToken).Handler()}
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
	log.Printf("weibo dashboard: %s  (image=%s, db=%s)", url, *image, *dbPath)
	if *authToken != "" {
		log.Print("weibo dashboard: API auth ENABLED — clients need -token / WEIBO_TOKEN")
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "weibo: serve: %v\n", err)
		return 1
	}
	log.Print("weibo dashboard: stopped")
	return 0
}

// makeBackend constructs and preflights the chosen container backend. On
// failure it prints a hint and returns (nil, exitCode).
func makeBackend(ctx context.Context, kind, image, namespace, kubeconfig string, pullSecrets []string, pvcSize, storageClass string) (backend.ContainerBackend, int) {
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
			fmt.Fprintf(os.Stderr, "weibo: runner image %q not found. Build it first:\n"+
				"  docker build -f Dockerfile.runner -t %s .\n", image, image)
			return nil, 1
		}
		return d, 0
	case "kubernetes", "k8s":
		kb, err := backend.NewKubernetes(backend.KubernetesOptions{
			Kubeconfig: kubeconfig, Namespace: namespace, Image: image,
			ImagePullSecrets: pullSecrets,
			PVCSize:          pvcSize, StorageClass: storageClass,
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
