package auth

// SASLMechanism identifies a Kafka SASL authentication mechanism.
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// SASLConfig holds Kafka SASL authentication settings.
// Used with the KafkaSASL (source) and KafkaSinkSASL (sink) options.
type SASLConfig struct {
	Mechanism SASLMechanism
	Username  string
	Password  string
}

// TLSConfig holds Kafka TLS settings.
//
// If CertFile and KeyFile are both set, client certificates are loaded
// for mutual TLS. If CAFile is set, it is added to the root CAs used to
// verify the broker. InsecureSkipVerify disables certificate verification
// (for development only).
type TLSConfig struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
}
