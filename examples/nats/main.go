package main

import (
	"log"
	"net/http"

	"mailer"
	"mailer/bus"
)

func main() {
	b, err := bus.NewNATS(bus.NATSConfig{
		URL:     "nats://localhost:4222",
		Subject: "mailer.>",
	})
	if err != nil {
		log.Fatal(err)
	}

	m := mailer.New(
		mailer.WithProjectID("dev_project"),
		mailer.WithAuditLimit(10000),
		mailer.WithBus(b),
	)
	defer m.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/events", m.PublishHandler)
	mux.HandleFunc("/ws", m.WebSocketHandler)
	mux.HandleFunc("/events-stream", m.SSEHandler)
	mux.HandleFunc("/audit-logs", m.AuditLogsHandler)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Println("Mailer example server running on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
