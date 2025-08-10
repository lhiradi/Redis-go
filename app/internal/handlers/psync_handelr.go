package handlers

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
)

var emptyRDB = []byte{
	0x52, 0x45, 0x44, 0x49, 0x53, // "REDIS"
	0x30, 0x30, 0x31, 0x31, // "0011"
	0xfa, 0x09, // "aux", keyspace size
	0xfb, 0x00, // "aux", expire time
	0xff,                                           // EOF
	0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Checksum
}

func handlePsync(conn net.Conn, args []string, DB *db.DB) error {
	fullResyncCmd := fmt.Sprintf("+FULLRESYNC %s %d\r\n", DB.ID, DB.Offset)
	_, err := conn.Write([]byte(fullResyncCmd))
	if err != nil {
		return fmt.Errorf("failed to send FULLRESYNC response: %w", err)
	}

	rdbFileHeader := fmt.Sprintf("$%d\r\n", len(emptyRDB))
	_, err = conn.Write([]byte(rdbFileHeader))
	if err != nil {
		return fmt.Errorf("failed to send RDB file header: %w", err)
	}
	_, err = conn.Write(emptyRDB)
	if err != nil {
		return fmt.Errorf("failed to send empty RDB file: %w", err)
	}
	return nil
}
