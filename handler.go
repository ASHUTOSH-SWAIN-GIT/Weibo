package mailer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

// PublishHandler accepts backend events and broadcasts them to subscribers.
func (m *Mailer) PublishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	var req PublishEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid json body",
		})
		return
	}

	req.Channel = strings.TrimSpace(req.Channel)
	req.Event = strings.TrimSpace(req.Event)

	if req.Channel == "" || req.Event == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "channel and event are required",
		})
		return
	}

	payload, err := json.Marshal(req.Payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "payload must be json serializable",
		})
		return
	}

	event := Event{
		ID:        newID(),
		ProjectID: m.config.ProjectID,
		Channel:   req.Channel,
		Name:      req.Event,
		Payload:   string(payload),
		CreatedAt: time.Now().UTC(),
	}

	delivered := m.hub.Broadcast(event)

	m.audit.Add(AuditLog{
		ID:        newID(),
		ProjectID: m.config.ProjectID,
		EventID:   event.ID,
		Action:    "EVENT_PUBLISHED",
		Message:   fmt.Sprintf("published %s on %s", event.Name, event.Channel),
		Metadata:  string(payload),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	writeJSON(w, http.StatusAccepted, PublishEventResponse{
		EventID:          event.ID,
		Broadcasted:      delivered > 0,
		DeliveredClients: delivered,
	})
}

// WebSocketHandler upgrades the connection and registers the client.
func (m *Mailer) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	channel := strings.TrimSpace(r.URL.Query().Get("channel"))
	if channel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "channel is required",
		})
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "connection closed")

	clientID := r.URL.Query().Get("clientId")
	if strings.TrimSpace(clientID) == "" {
		clientID = newID()
	}

	client := NewClient(clientID, conn)
	m.hub.Subscribe(channel, client)
	defer m.hub.RemoveClient(client)

	m.audit.Add(AuditLog{
		ID:        newID(),
		ProjectID: m.config.ProjectID,
		Action:    "CLIENT_CONNECTED",
		Message:   fmt.Sprintf("client %s subscribed to %s", clientID, channel),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go client.WriteLoop(ctx)

	for {
		if _, _, err := conn.Read(ctx); err != nil {
			m.audit.Add(AuditLog{
				ID:        newID(),
				ProjectID: m.config.ProjectID,
				Action:    "CLIENT_DISCONNECTED",
				Message:   fmt.Sprintf("client %s disconnected", clientID),
				CreatedAt: time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
	}
}

// AuditHandler returns in-memory audit logs for the configured project.
func (m *Mailer) AuditHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	projectID := strings.TrimSpace(r.URL.Query().Get("projectId"))
	if projectID == "" {
		projectID = m.config.ProjectID
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"logs": m.audit.List(projectID),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
