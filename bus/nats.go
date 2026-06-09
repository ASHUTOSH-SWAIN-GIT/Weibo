package bus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

type NATS struct {
	conn   *nats.Conn
	sub    *nats.Subscription
	subject string
	events chan Event
	done   chan struct{}
}

type NATSConfig struct {
	URL      string
	Subject  string
}

func NewNATS(cfg NATSConfig) (*NATS, error) {
	if cfg.URL == "" {
		cfg.URL = nats.DefaultURL
	}
	if cfg.Subject == "" {
		cfg.Subject = "mailer.>"
	}

	conn, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	b := &NATS{
		conn:    conn,
		subject: cfg.Subject,
		events:  make(chan Event, 256),
		done:    make(chan struct{}),
	}

	sub, err := conn.Subscribe(b.subject, func(msg *nats.Msg) {
		var event Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return
		}
		select {
		case b.events <- event:
		default:
		}
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("nats subscribe: %w", err)
	}
	b.sub = sub

	return b, nil
}

func (b *NATS) Publish(ctx context.Context, channel string, event Event) error {
	event.Channel = channel
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	subject := "mailer." + channel
	return b.conn.Publish(subject, data)
}

func (b *NATS) Events() <-chan Event {
	return b.events
}

func (b *NATS) Close() error {
	close(b.done)
	if b.sub != nil {
		b.sub.Unsubscribe()
	}
	b.conn.Close()
	return nil
}
