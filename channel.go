package mailer

import "context"

type Channel struct {
	name   string
	mailer *Mailer
}

func (c *Channel) Publish(ctx context.Context, eventName string, payload any) (string, error) {
	return c.mailer.publish(ctx, c.name, eventName, payload)
}

func (c *Channel) Subscribe(handler Handler) int {
	return c.mailer.hub.Subscribe(c.name, handler)
}

func (c *Channel) Unsubscribe(id int) {
	c.mailer.hub.Unsubscribe(c.name, id)
}
