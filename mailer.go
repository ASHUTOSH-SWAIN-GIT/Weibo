package mailer

// config maintains mailer package configuration
type Config struct {
	//projectid is used for audit logs
	// later this can be come fro  api keys or tenant Config
	ProjectID string

	//auditlimit controls how many audit logs are stored in memory
	AuditLimit int
}

type Mailer struct {
	config Config
	hub    *Hub
	audit  *AuditStore
}

// new creates a new mailer instance
func New(config Config) *Mailer {
	if config.ProjectID == "" {
		config.ProjectID = "default_project"
	}

	if config.AuditLimit <= 0 {
		config.AuditLimit = 1000
	}
	return &Mailer{
		config: config,
		hub:    NewHub(),
		audit:  NewAuditStore(config.AuditLimit),
	}
}
