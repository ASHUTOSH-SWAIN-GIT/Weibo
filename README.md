# Mailer

Mailer is a simple realtime event gateway built in Go.

The idea is to let users send events from their backend using an SDK/package, pass those events through a central event streaming gateway, store audit logs in memory, and deliver the events to connected clients in realtime.

This is not meant to be a full message broker like Kafka, NATS, or RabbitMQ. It is a lightweight project focused on learning and building a simple SDK-first realtime event system.

## Project Description

Mailer allows a backend application to send events like:

```json
{
  "channel": "orders",
  "event": "order.created",
  "payload": {
    "orderId": "ord_123",
    "amount": 499
  }
}
```

The gateway receives the event, stores an audit log, and forwards the event to connected clients through a streaming layer such as WebSocket or SSE.

Users can also configure their own database later if they want events to be saved outside the gateway.

## Architecture

```txt
Client / Backend SDK
        |
        | send event
        v
Event Streaming Gateway
        |
        |-- In-memory Audit DB
        |
        |-- Streaming Gateway / Realtime Layer
                    |
                    | send realtime updates
                    v
              Frontend / Client SDK
```

## How It Works

1. The user installs or uses the SDK/package in their backend.
2. The backend sends an event to the Event Streaming Gateway.
3. The gateway receives and validates the event.
4. The gateway stores an audit log in memory.
5. The gateway sends the event to the streaming layer.
6. Connected clients receive the event in realtime using the frontend/client SDK.

## Main Components

### Backend SDK

Used by the user’s backend to send events.

Example:

```go
gateway.Publish("orders", "order.created", payload)
```

### Event Streaming Gateway

The main Go service/package that receives events, manages routing, keeps audit logs, and sends events to the realtime layer.

### In-memory Audit DB

Stores temporary audit logs such as:

```txt
EVENT_RECEIVED
EVENT_BROADCASTED
CLIENT_CONNECTED
CLIENT_DISCONNECTED
DB_WRITE_SUCCESS
DB_WRITE_FAILED
```

These logs are stored in memory for now, so they are not permanent.

### Streaming Gateway / Realtime Layer

Handles realtime delivery to connected clients using WebSocket or SSE.

### Frontend / Client SDK

Used by frontend applications to receive realtime event updates.

Example:

```ts
client.subscribe("orders", (event) => {
  console.log(event)
})
```

## Current Goal

The first version will focus on:

* Sending events from a backend SDK/package
* Receiving events in the Go gateway
* Storing audit logs in memory
* Broadcasting events to connected clients
* Keeping the system simple and easy to understand

## Future Improvements

Possible future features:

* User database connector
* PostgreSQL event storage
* Retry system for failed writes
* WebSocket reconnect support
* API key authentication
* Channel-based permissions
* Multiple gateway instances using Redis or NATS
* TypeScript frontend SDK

## Status

This project is currently in the early development stage.

The goal is to learn Go, realtime systems, WebSockets, SDK design, and basic event streaming architecture.
