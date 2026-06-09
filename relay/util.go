package relay

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func newRelayID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
