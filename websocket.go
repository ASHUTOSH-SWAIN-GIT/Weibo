package mailer

import (
	"context"
	"encoding/json"
	"time"

	"nhooyr.io/websocket"
)

// client represents one connected client
type Client struct {
	id       string
	conn     *websocket.Conn
	channels map[string]bool
	// this is an internal buffered channel
	// the hub writes events into this channel
	// the writeloop reads from this channel and sends to websocket
	send chan Event
}

// newclient creates a new websocket client wrapper
func NewClient(id string, conn *websocket.Conn) *Client {
	return &Client{
		id:       id,
		conn:     conn,
		channels: make(map[string]bool),
		//buffered channel prevents slow clients from blocking
		// the whole hub immediately
		send: make(chan Event, 64),
	}
}

// send queues an event for this client
// it does not directly write to websocket
// it pushes the event into the send channel
// if the channel ils full it returns false
func (c *Client) Send(event Event) bool {
	select {
	case c.send <- event:
		return true
	default:
		return false
	}
}

// writeloop continuously listens for events from c.send
// and writes them to the websocket connection
func (c *Client) WriteLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case event := <-c.send:
			// Each write has a timeout so a stuck client does not hang forever.
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

			// This is the message format frontend clients receive.
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
