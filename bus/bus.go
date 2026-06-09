package bus

import (
	"context"
)

type Event struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	Channel   string `json:"channel"`
	Name      string `json:"event"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"createdAt"`
}

type Bus interface {
	Publish(ctx context.Context, channel string, event Event) error
	Events() <-chan Event
	Close() error
}
