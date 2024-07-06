package database

import "fmt"

const (
	TypeString = "string"
	TypeStream = "stream"
	// TypeList   = "list"
	// ...
)

func StreamEntryID(ts, seq uint64) string {
	return fmt.Sprintf("%d-%d", ts, seq)
}
