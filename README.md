# Mailer

Mailer is a real-time event streaming SDK for Go. Publish events from your backend, deliver them to subscribers instantly via WebSocket or SSE, and scale across multiple gateway instances using Redis or NATS as the backbone bus.

## Architecture

```
Publisher (SDK)  ──→  Gateway A  ──→  Bus (Redis / NATS / InMem)
                                            │
                            ┌───────────────┼───────────────┐
                            ↓               ↓               ↓
                        Gateway A       Gateway B       Gateway C
                         │      │        │      │        │      │
                      Hub    Relay    Hub    Relay    Hub    Relay
                       │    WS SSE     │    WS SSE     │    WS SSE
                  Go callbacks  Browser clients  Go callbacks  Browser clients
```

**Publish** goes through the bus → fans out to all gateways → each gateway delivers to its local Hub callbacks + Relay clients.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "net/http"

    "mailer"
    "mailer/bus"
)

func main() {
    m := mailer.New(
        mailer.WithProjectID("my-app"),
        mailer.WithBus(bus.NewInMem()),
    )
    defer m.Close()

    // Subscribe via SDK (Go callbacks)
    m.Subscribe("orders", func(e mailer.Event) {
        fmt.Printf("received: %s — %s\n", e.Name, e.Payload)
    })

    // Publish an event
    eventID, _ := m.Publish(context.Background(), "orders", "order.created", map[string]any{
        "orderId": "ord_123",
        "amount":  499,
    })
    fmt.Println("published:", eventID)

    // Mount relay handlers for browser clients
    http.Handle("/ws", m.WebSocketHandler())
    http.Handle("/events", m.SSEHandler())
    http.ListenAndServe(":8080", nil)
}
```

## SDK API

### Method-based

```go
// Publish
eventID, err := m.Publish(ctx, "orders", "order.created", payload)

// Subscribe (returns subscription ID)
id := m.Subscribe("orders", func(e mailer.Event) { ... })

// Unsubscribe
m.Unsubscribe("orders", id)
```

### Channel-based

```go
ch := m.Channel("orders")

eventID, err := ch.Publish(ctx, "order.created", payload)

id := ch.Subscribe(func(e mailer.Event) { ... })
ch.Unsubscribe(id)
```

### Wildcard

```go
// Subscribe to all channels
m.Subscribe("*", func(e mailer.Event) { ... })
```

## Bus Backbones

### In-Memory (single gateway)

```go
m := mailer.New(
    mailer.WithBus(bus.NewInMem()),
)
```

### Redis Pub/Sub (multi-gateway)

```go
b := bus.NewRedis(bus.RedisConfig{
    Addr:    "localhost:6379",
    Channel: "mailer",
})
m := mailer.New(mailer.WithBus(b))
```

### NATS (multi-gateway)

```go
b, err := bus.NewNATS(bus.NATSConfig{
    URL:     "nats://localhost:4222",
    Subject: "mailer.>",
})
m := mailer.New(mailer.WithBus(b))
```

## Browser Clients

### WebSocket

```js
const ws = new WebSocket("ws://localhost:8080/ws?channel=orders");
ws.onmessage = (msg) => {
  const event = JSON.parse(msg.data);
  console.log(event.event, event.payload);
};
```

### SSE

```js
const source = new EventSource("http://localhost:8080/events?channel=orders");
source.onmessage = (msg) => {
  const event = JSON.parse(msg.data);
  console.log(event.event, event.payload);
};
```

### Message Format (both transports)

```json
{
  "type": "event",
  "id": "a1b2c3d4...",
  "channel": "orders",
  "event": "order.created",
  "payload": "{\"orderId\":\"ord_123\",\"amount\":499}",
  "createdAt": "2026-06-09T12:00:00Z"
}
```

## Audit Logs

```go
logs := m.AuditLogs()
```

Audit logs are stored in-memory with a configurable limit. Actions tracked:

- `EVENT_PUBLISHED` — event received from publisher
- `EVENT_DELIVERED` — event dispatched to local subscribers

## Configuration

```go
m := mailer.New(
    mailer.WithProjectID("my-app"),
    mailer.WithAuditLimit(5000),
    mailer.WithBus(bus.NewInMem()),
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithProjectID` | `"default_project"` | Project identifier for audit logs |
| `WithAuditLimit` | `1000` | Max audit logs stored in memory |
| `WithBus` | `bus.NewInMem()` | Bus backbone for inter-gateway communication |

## Package Structure

```
mailer/
├── mailer.go          # Core: Mailer, New(), Publish(), Subscribe(), Channel()
├── channel.go         # Channel type with Publish/Subscribe
├── hub.go             # Local Go callback dispatcher
├── event.go           # Event type
├── audit.go           # In-memory audit store
├── bus/
│   ├── bus.go         # Bus interface
│   ├── inmem.go       # In-memory bus (single gateway)
│   ├── redis.go       # Redis Pub/Sub bus
│   └── nats.go        # NATS bus
├── relay/
│   ├── relay.go       # Relay interface + Event type
│   ├── manager.go     # Manager: fans out to both relays
│   ├── websocket.go   # WebSocket relay
│   └── sse.go         # SSE relay
└── examples/
    ├── basic/         # Single gateway with InMem bus
    ├── redis/         # Multi-gateway with Redis
    └── nats/          # Multi-gateway with NATS
```

## Status

Early development. See future improvements below.

## Future Improvements

- Authentication (API keys, middleware hooks)
- Channel-based permissions
- PostgreSQL event storage
- Retry system for failed deliveries
- WebSocket reconnect with event replay
- TypeScript frontend SDK
- Metrics and monitoring
- Clustering with consistent hashing
