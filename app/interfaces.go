package main

type reader interface {
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}
