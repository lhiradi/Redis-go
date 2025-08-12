package db

import "sync"

type Store struct {
	Data    map[string]cacheValue
	Streams map[string][]StreamEntry
	Mu      sync.RWMutex
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type StreamReadEntry struct {
	Key     string
	Entries []StreamEntry
}

type cacheValue struct {
	Value string
	Ttl   int64
}