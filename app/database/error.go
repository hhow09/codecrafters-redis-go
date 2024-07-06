package database

import "errors"

var (
	ErrWrongType      = errors.New("wrong data type")
	ErrInvalidEntryID = errors.New("invalid entry id")
	ErrIDMinVal       = errors.New("The ID specified in XADD must be greater than 0-0")
	ErrIDTooSmall     = errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
)
