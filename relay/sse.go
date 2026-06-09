package relay

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type sseClient struct {
	id       string
	channels map[string]bool
	send     chan Event
}

type SSERelay struct {
	mu      sync.RWMutex
	clients map[string]*sseClient
}

func NewSSERelay() *SSERelay {
	return &SSERelay{
		clients: make(map[string]*sseClient),
	}
}

func (r *SSERelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	channel := strings.TrimSpace(req.URL.Query().Get("channel"))
	if channel == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "channel is required"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	clientID := strings.TrimSpace(req.URL.Query().Get("clientId"))
	if clientID == "" {
		clientID = newRelayID()
	}

	client := &sseClient{
		id:       clientID,
		channels: map[string]bool{channel: true},
		send:     make(chan Event, 64),
	}

	r.mu.Lock()
	r.clients[clientID] = client
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.clients, clientID)
		r.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ctx := req.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-client.send:
			msg := map[string]any{
				"type":      "event",
				"id":        event.ID,
				"channel":   event.Channel,
				"event":     event.Name,
				"payload":   event.Payload,
				"createdAt": event.CreatedAt,
			}

			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (r *SSERelay) Broadcast(event Event) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, client := range r.clients {
		if client.channels[event.Channel] || client.channels["*"] {
			select {
			case client.send <- event:
			default:
			}
		}
	}
}

func (r *SSERelay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id := range r.clients {
		delete(r.clients, id)
	}
}
