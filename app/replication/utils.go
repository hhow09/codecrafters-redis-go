package replication

import (
	"math/rand"
	"strings"
)

func GenReplicationID() string {
	const idlen = 40
	const alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var sb strings.Builder
	sb.Grow(idlen) // Pre-allocate memory for efficiency

	for i := 0; i < idlen; i++ {
		index := rand.Intn(len(alphanumericChars))
		sb.WriteByte(alphanumericChars[index])
	}

	return sb.String()
}
