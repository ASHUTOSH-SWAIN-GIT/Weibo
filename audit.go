package mailer

import (
	"sync"
)

type AuditLog struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	EventID   string `json:"eventID,omitempty"`
	Action    string `json:"action"`
	Message   string `json:"message"`
	Metadata  string `json:"metadata,omitempty"`
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
	return &AuditStore{
		logs:  make([]AuditLog, 0),
		limit: limit,
	}
}

func (s *AuditStore) Add(log AuditLog) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, log)

	if len(s.logs) > s.limit {
		s.logs = s.logs[len(s.logs)-s.limit:]
	}
}

func (s *AuditStore) List(projectID string) []AuditLog {
	s.mu.RLock()
	defer s.mu.Unlock()

	result := make([]AuditLog, 0)

	for _, log := range s.logs {
		if projectID == "" || log.ProjectID == projectID {
			result = append(result, log)
		}
	}
	return result
}
