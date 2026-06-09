package bus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client    *redis.Client
	channel   string
	events    chan Event
	sub       *redis.PubSub
	done      chan struct{}
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Channel  string
}

func NewRedis(cfg RedisConfig) *Redis {
	if cfg.Channel == "" {
		cfg.Channel = "mailer"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	b := &Redis{
		client:  client,
		channel: cfg.Channel,
		events:  make(chan Event, 256),
		done:    make(chan struct{}),
	}

	go b.receive()

	return b
}

func (b *Redis) Publish(ctx context.Context, channel string, event Event) error {
	event.Channel = channel
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return b.client.Publish(ctx, b.channel, data).Err()
}

func (b *Redis) Events() <-chan Event {
	return b.events
}

func (b *Redis) Close() error {
	close(b.done)
	if b.sub != nil {
		b.sub.Close()
	}
	return b.client.Close()
}

func (b *Redis) receive() {
	b.sub = b.client.Subscribe(context.Background(), b.channel)

	ch := b.sub.Channel()

	for {
		select {
		case <-b.done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var event Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				continue
			}
			select {
			case b.events <- event:
			default:
			}
		}
	}
}
