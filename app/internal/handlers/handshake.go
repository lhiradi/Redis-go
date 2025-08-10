package handlers

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

func sendAndReceiveOK(conn net.Conn, reader *bufio.Reader, command string) error {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

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

func HandshakeWithMaster(conn net.Conn, port string) error {
	reader := bufio.NewReader(conn)

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	_, err := conn.Write([]byte(pingCommand))
	if err != nil {
		return fmt.Errorf("failed to send PING to master: %w", err)
	}

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
	if err := sendAndReceiveOK(conn, reader, replconfPortCmd); err != nil {
		return fmt.Errorf("REPLCONF listening-port failed: %w", err)
	}

	replconfCapaCmd := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	if err := sendAndReceiveOK(conn, reader, replconfCapaCmd); err != nil {
		return fmt.Errorf("REPLCONF capa psync2 failed: %w", err)
	}

	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	if _, err := conn.Write([]byte(psyncCmd)); err != nil {
		return fmt.Errorf("PSYNC ? -1 failed: %w", err)
	}

	// Read and discard FULLRESYNC and RDB file.
	fullResyncResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read FULLRESYNC response: %w", err)
	}
	if !strings.HasPrefix(fullResyncResp, "+FULLRESYNC") {
		return fmt.Errorf("expected FULLRESYNC, got: %s", fullResyncResp)
	}

	rdbFileHeader, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB file header: %w", err)
	}
	if !strings.HasPrefix(rdbFileHeader, "$") {
		return fmt.Errorf("expected RDB file header, got: %s", rdbFileHeader)
	}

	var rdbLen int
	if _, err := fmt.Sscanf(rdbFileHeader, "$%d\r\n", &rdbLen); err != nil {
		return fmt.Errorf("failed to parse RDB file length: %w", err)
	}

	rdbData := make([]byte, rdbLen)
	if _, err := io.ReadFull(reader, rdbData); err != nil {
		return fmt.Errorf("failed to read RDB file content: %w", err)
	}

	fmt.Println("Handshake with master successful.")
	return nil
}
