package utils

import (
	"crypto/rand"
	"fmt"
	"io"
)

func GenerateReplicaID() string {
	bytes := make([]byte, 20) // 20 bytes = 40 hex characters
	if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
		return ""
	}

	return fmt.Sprintf("%x", bytes)
}
