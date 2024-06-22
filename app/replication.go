package main

import (
	"math/rand"
	"strings"
)

func generateRandomString(n int) string {
	const alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var sb strings.Builder
	sb.Grow(n) // Pre-allocate memory for efficiency

	for i := 0; i < n; i++ {
		index := rand.Intn(len(alphanumericChars))
		sb.WriteByte(alphanumericChars[index])
	}

	return sb.String()
}
