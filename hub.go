package mailer

import (
	"sync"
)

// This manages all the realtime subscription
// channel -> connected clients
type Hub struct {
	mu       sync.RWMutex
	channels map[string]map[*Client]bool
}

func NewHub() *Hub {
	return &Hub{
		channels: make(map[string]map[*Client]bool),
	}
}

// Subscribe adds a client to the channel and it will be received by the
// client whenever that event is published
func (h *Hub) Subscribe(channel string, client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.channels[channel] == nil {
		h.channels[channel] = make(map[*Client]bool)
	}

	h.channels[channel][client] = true
	client.channels[channel] = true
}

func (h *Hub) Unsubscribe(channel string, client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.channels[channel] != nil {
		delete(h.channels[channel], client)

		if len(h.channels[channel]) == 0 {
			delete(h.channels, channel)
		}
	}

	delete(client.channels, channel)
}

// this removes a disconnected client from all channels
// unlike the unsubscribe it removes the client from a specific channel
func (h *Hub) RemoveClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for channel := range client.channels {
		if h.channels[channel] != nil {
			delete(h.channels[channel], client)

			if len(h.channels[channel]) == 0 {
				delete(h.channels, channel)
			}
		}
	}

	client.channels = make(map[string]bool)
}

// Broadcasts the event to all the subscibed clients
// it returns how many clients were successfully queued for delivery
func (h *Hub) Broadcast(event Event) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	subscribers := h.channels[event.Channel]
	delivered := 0

	for client := range subscribers {
		if client.Send(event) {
			delivered++
		}
	}

	return delivered
}
