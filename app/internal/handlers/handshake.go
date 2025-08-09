package handlers

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

func HandshakeWithMaster(conn net.Conn) error {
	pingCommand := "*1\r\n$4\r\nPING\r\n"

	_, err := conn.Write([]byte(pingCommand))
	if err != nil {
		return fmt.Errorf("failed to send PING to master: %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("connection closed by master before receiving PONG")
		}
		return fmt.Errorf("failed to read response from master: %w", err)
	}
	if strings.TrimSpace(response) != "+PONG" {
		return fmt.Errorf("master did not respond with PONG, got: %s", response)
	}

	fmt.Println("Received PONG from master. Handshake successful.")
	return nil
}
