package relay

type Manager struct {
	ws  *WebSocketRelay
	sse *SSERelay
}

func NewManager() *Manager {
	return &Manager{
		ws:  NewWebSocketRelay(),
		sse: NewSSERelay(),
	}
}

func (m *Manager) Broadcast(event Event) {
	m.ws.Broadcast(event)
	m.sse.Broadcast(event)
}

func (m *Manager) WebSocket() *WebSocketRelay {
	return m.ws
}

func (m *Manager) SSE() *SSERelay {
	return m.sse
}

func (m *Manager) Close() {
	m.ws.Close()
	m.sse.Close()
}
