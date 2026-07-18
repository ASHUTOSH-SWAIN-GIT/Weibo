// Command mailer-controller is the Mailer job control plane. It exposes a
// REST API to submit and manage workflow jobs, launches one runner
// container per job via Docker, persists state to SQLite, and reconciles
// containers toward each job's desired state.
//
//	mailer-controller \
//	  -addr :9000 \
//	  -db ./mailer-control.db \
//	  -image mailer-runner:dev
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

func main() {
	addr := flag.String("addr", ":9000", "API listen address")
	dbPath := flag.String("db", "./mailer-control.db", "SQLite database path")
	image := flag.String("image", "mailer-runner:dev", "runner image tag")
	interval := flag.Duration("reconcile", 3*time.Second, "reconcile interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.OpenSQLite(*dbPath)
	if err != nil {
		log.Fatalf("controller: open store: %v", err)
	}
	defer st.Close()

	docker, err := backend.NewDocker(*image)
	if err != nil {
		log.Fatalf("controller: docker: %v", err)
	}
	if err := docker.Ping(ctx); err != nil {
		log.Fatalf("controller: docker daemon unreachable: %v", err)
	}

	ctrl := control.New(control.Options{
		Store:   st,
		Backend: docker,
		Image:   *image,
		Logf:    log.Printf,
	})

	// Reconcile in the background: adopt existing containers on startup
	// and keep them converging on desired state.
	go ctrl.RunReconciler(ctx, *interval)

	srv := &http.Server{Addr: *addr, Handler: api.NewServer(ctrl).Handler()}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Printf("controller: listening on %s (image=%s db=%s)", *addr, *image, *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("controller: serve: %v", err)
	}
	log.Print("controller: stopped")
}
