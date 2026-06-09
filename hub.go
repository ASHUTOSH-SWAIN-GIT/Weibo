package mailer

import (
	"sync"
)

type Handler func(Event)

type Hub struct {
	mu       sync.RWMutex
	handlers map[string]map[int]Handler
	nextID   int
}

func NewHub() *Hub {
	return &Hub{
		handlers: make(map[string]map[int]Handler),
	}
}

func (h *Hub) Subscribe(channel string, handler Handler) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++

	if h.handlers[channel] == nil {
		h.handlers[channel] = make(map[int]Handler)
	}
	h.handlers[channel][id] = handler

	return id
}

func (h *Hub) Unsubscribe(channel string, id int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.handlers[channel] != nil {
		delete(h.handlers[channel], id)
		if len(h.handlers[channel]) == 0 {
			delete(h.handlers, channel)
		}
	}
}

func (h *Hub) Dispatch(event Event) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	dispatched := 0
	for _, handler := range h.handlers[event.Channel] {
		handler(event)
		dispatched++
	}

	for _, handler := range h.handlers["*"] {
		handler(event)
		dispatched++
	}

	return dispatched
}
