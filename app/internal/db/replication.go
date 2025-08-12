package db

import (
	"net"
	"sync"
)

type Replication struct {
	ID              string
	Offset          int
	Replicas        []*ReplicaConn
	ReplicaMu       sync.RWMutex
	NumAcksRecieved int64
}

type ReplicaConn struct {
	Conn net.Conn
	Mu   sync.Mutex
}
