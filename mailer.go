package mailer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"mailer/bus"
	"mailer/relay"
)

type Config struct {
	ProjectID  string
	AuditLimit int
}

type Mailer struct {
	config Config
	hub    *Hub
	audit  *AuditStore
	bus    bus.Bus
	relay  *relay.Manager
	cancel context.CancelFunc
}

type Option func(*Mailer)

func WithProjectID(id string) Option {
	return func(m *Mailer) {
		m.config.ProjectID = id
	}
}

func WithAuditLimit(limit int) Option {
	return func(m *Mailer) {
		m.config.AuditLimit = limit
	}
}

func WithBus(b bus.Bus) Option {
	return func(m *Mailer) {
		m.bus = b
	}
}

func New(opts ...Option) *Mailer {
	m := &Mailer{
		config: Config{
			ProjectID:  "default_project",
			AuditLimit: 1000,
		},
		hub:   NewHub(),
		audit: NewAuditStore(1000),
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.bus == nil {
		m.bus = bus.NewInMem()
	}

	m.relay = relay.NewManager()

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go m.eventLoop(ctx)

	return m
}

func (m *Mailer) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case bEvent := <-m.bus.Events():
			event := busEventToEvent(bEvent)

			m.hub.Dispatch(event)

			m.relay.Broadcast(eventToRelayEvent(event))

			m.audit.Add(AuditLog{
				ID:        newEventID(),
				ProjectID: event.ProjectID,
				EventID:   event.ID,
				Action:    "EVENT_DELIVERED",
				Message:   fmt.Sprintf("delivered %s on %s", event.Name, event.Channel),
				CreatedAt: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}
}

func (m *Mailer) publish(ctx context.Context, channel, eventName string, payload any) (string, error) {
	p := marshalPayload(payload)

	event := Event{
		ID:        newEventID(),
		ProjectID: m.config.ProjectID,
		Channel:   channel,
		Name:      eventName,
		Payload:   p,
		CreatedAt: time.Now().UTC(),
	}

	bEvent := eventToBusEvent(event)
	if err := m.bus.Publish(ctx, channel, bEvent); err != nil {
		return "", fmt.Errorf("bus publish failed: %w", err)
	}

	m.audit.Add(AuditLog{
		ID:        newEventID(),
		ProjectID: m.config.ProjectID,
		EventID:   event.ID,
		Action:    "EVENT_PUBLISHED",
		Message:   fmt.Sprintf("published %s on %s", event.Name, event.Channel),
		Metadata:  p,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	return event.ID, nil
}

func (m *Mailer) Publish(ctx context.Context, channel, eventName string, payload any) (string, error) {
	return m.publish(ctx, channel, eventName, payload)
}

func (m *Mailer) Subscribe(channel string, handler Handler) int {
	return m.hub.Subscribe(channel, handler)
}

func (m *Mailer) Unsubscribe(channel string, id int) {
	m.hub.Unsubscribe(channel, id)
}

func (m *Mailer) Channel(name string) *Channel {
	return &Channel{name: name, mailer: m}
}

func (m *Mailer) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	m.relay.WebSocket().ServeHTTP(w, r)
}

func (m *Mailer) SSEHandler(w http.ResponseWriter, r *http.Request) {
	m.relay.SSE().ServeHTTP(w, r)
}

func (m *Mailer) PublishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	var req PublishEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	if req.Channel == "" || req.Event == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "channel and event are required"})
		return
	}

	eventID, err := m.publish(r.Context(), req.Channel, req.Event, req.Payload)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}

	writeJSON(w, http.StatusAccepted, PublishEventResponse{
		EventID:     eventID,
		Broadcasted: true,
	})
}

func (m *Mailer) AuditLogsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"logs": m.audit.List(m.config.ProjectID),
	})
}

func (m *Mailer) AuditLogs() []AuditLog {
	return m.audit.List(m.config.ProjectID)
}

func (m *Mailer) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.relay.Close()
	return m.bus.Close()
}

func eventToBusEvent(e Event) bus.Event {
	return bus.Event{
		ID:        e.ID,
		ProjectID: e.ProjectID,
		Channel:   e.Channel,
		Name:      e.Name,
		Payload:   e.Payload,
		CreatedAt: e.CreatedAt.Format(time.RFC3339),
	}
}

func busEventToEvent(b bus.Event) Event {
	t, _ := time.Parse(time.RFC3339, b.CreatedAt)
	return Event{
		ID:        b.ID,
		ProjectID: b.ProjectID,
		Channel:   b.Channel,
		Name:      b.Name,
		Payload:   b.Payload,
		CreatedAt: t,
	}
}

func eventToRelayEvent(e Event) relay.Event {
	return relay.Event{
		ID:        e.ID,
		ProjectID: e.ProjectID,
		Channel:   e.Channel,
		Name:      e.Name,
		Payload:   e.Payload,
		CreatedAt: e.CreatedAt.Format(time.RFC3339),
	}
}

func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func marshalPayload(payload any) string {
	data, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
