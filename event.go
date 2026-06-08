package mailer

import "time"

type Event struct {
	ID        string    `json:"id"`
	ProjectID string    `json:"projectId"`
	Channel   string    `json:"channel"`
	Name      string    `json:"event"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"createdAt"`
}

type PublishEventRequest struct {
	Channel string         `json:"channel"`
	Event   string         `json:"event"`
	Payload map[string]any `json:"payload"`
}

type PublishEventResponse struct {
	EventID          string `json:"eventId"`
	Broadcasted      bool   `json:"broadcasted"`
	DeliveredClients int    `json:'deliveredClients`
}
