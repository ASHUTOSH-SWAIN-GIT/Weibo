package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

type wsClient struct {
	id       string
	conn     *websocket.Conn
	channels map[string]bool
	send     chan Event
}

type WebSocketRelay struct {
	mu      sync.RWMutex
	clients map[string]*wsClient
}

func NewWebSocketRelay() *WebSocketRelay {
	return &WebSocketRelay{
		clients: make(map[string]*wsClient),
	}
}

func (r *WebSocketRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	channel := strings.TrimSpace(req.URL.Query().Get("channel"))
	if channel == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "channel is required"})
		return
	}

	conn, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "connection closed")

	clientID := strings.TrimSpace(req.URL.Query().Get("clientId"))
	if clientID == "" {
		clientID = newRelayID()
	}

	client := &wsClient{
		id:       clientID,
		conn:     conn,
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

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	go client.writeLoop(ctx)

	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
	}
}

func (r *WebSocketRelay) Broadcast(event Event) {
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

func (r *WebSocketRelay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, client := range r.clients {
		client.conn.Close(websocket.StatusNormalClosure, "server shutdown")
		delete(r.clients, id)
	}
}

func (c *wsClient) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-c.send:
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

			msg := map[string]any{
				"type":      "event",
				"id":        event.ID,
				"channel":   event.Channel,
				"event":     event.Name,
				"payload":   event.Payload,
				"createdAt": event.CreatedAt,
			}

			data, err := json.Marshal(msg)
			if err == nil {
				_ = c.conn.Write(writeCtx, websocket.MessageText, data)
			}
			cancel()
		}
	}
}
