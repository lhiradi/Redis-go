package handlers

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

func sendAndReceiveOK(conn net.Conn, command string) error {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("connection closed by master before receiving +OK")
		}
		return fmt.Errorf("failed to read response from master: %w", err)
	}
	if strings.TrimSpace(response) != "+OK" {
		return fmt.Errorf("master did not respond with +OK, got: %s", response)
	}
	return nil
}

// HandshakeWithMaster performs the full handshake with the master.
func HandshakeWithMaster(conn net.Conn, port string) error {
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

	replconfPortCmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)
	if err := sendAndReceiveOK(conn, replconfPortCmd); err != nil {
		return fmt.Errorf("REPLCONF listening-port failed: %w", err)
	}

	replconfCapaCmd := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	if err := sendAndReceiveOK(conn, replconfCapaCmd); err != nil {
		return fmt.Errorf("REPLCONF capa psync2 failed: %w", err)
	}

	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	if _, err := conn.Write([]byte(psyncCmd)); err != nil {
		return fmt.Errorf("PSYNC ? -1 failed: %w", err)
	}

	fmt.Println("Handshake with master successful.")
	return nil
}
