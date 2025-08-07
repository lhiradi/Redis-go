package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()
	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleClient(conn)
	}

}

func handleClient(conn net.Conn) {

	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection")
			return
		}

		if string(buf[:]) == "PING" {

			conn.Write([]byte("+PONG\r\n"))

		} else if string(buf[:4]) == "ECHO" {
			conn.Write([]byte(string(buf[4:])))
		}

	}
}
