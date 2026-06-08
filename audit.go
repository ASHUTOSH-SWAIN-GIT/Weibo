package mailer

import "sync"

type AuditLog struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	EventID   string `json:"eventID,omitempty"`
	Action    string `json:"action"`
	Message   string `json:"message"`
	Metadata  string `json:"metadata,omitemply"`
	CreatedAt string `json:"createdAt"`
}

type AuditStore struct {
	mu    sync.RWMutex
	logs  []AuditLog
	limit int
}

func NewAuditStore(limit int) *AuditStore {
	if limit <= 0 {
		limit = 10000
	}

}
