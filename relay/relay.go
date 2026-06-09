package relay

type Event struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	Channel   string `json:"channel"`
	Name      string `json:"event"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"createdAt"`
}

type Relay interface {
	Broadcast(event Event)
	Close()
}
