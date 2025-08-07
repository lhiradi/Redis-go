package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type DB struct {
	data map[string]string
}

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
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	db := &DB{
		make(map[string]string),
	}

	reader := bufio.NewReader(conn)

	for {
		// Read the RESP array length (*2\r\n)
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Print(err)
			return
		}

		// Check if it's a valid RESP array
		if len(line) < 2 || line[0] != '*' {
			log.Printf("Invalid RESP format: %s", line)
			return
		}

		// Parse the number of elements in the array
		var arrayLength int
		_, err = fmt.Sscanf(line, "*%d\r\n", &arrayLength)
		if err != nil {
			log.Printf("Failed to parse array length: %v", err)
			return
		}

		// Read the command and its arguments
		args := make([]string, arrayLength)
		for i := 0; i < arrayLength; i++ {
			// Read the bulk string length ($n\r\n)
			lengthLine, err := reader.ReadString('\n')
			if err != nil {
				log.Print(err)
				return
			}

			if len(lengthLine) < 2 || lengthLine[0] != '$' {
				log.Printf("Invalid bulk string format: %s", lengthLine)
				return
			}

			// Parse the bulk string length
			var strLen int
			_, err = fmt.Sscanf(lengthLine, "$%d\r\n", &strLen)
			if err != nil {
				log.Printf("Failed to parse string length: %v", err)
				return
			}

			// Read the actual string content
			arg := make([]byte, strLen+2) // +2 for \r\n
			_, err = io.ReadFull(reader, arg)
			if err != nil {
				log.Print(err)
				return
			}

			// Store the argument (without \r\n)
			args[i] = string(arg[:strLen])
		}

		// Process the command
		if len(args) > 0 {
			command := strings.ToUpper(args[0])
			switch command {
			case "PING":
				if len(args) == 1 {
					// PING without arguments returns "PONG"
					conn.Write([]byte("+PONG\r\n"))
				} else {
					// PING with argument returns the argument
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
					conn.Write([]byte(response))
				}
			case "ECHO":
				if len(args) >= 2 {
					// ECHO returns the second argument as a bulk string
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
					conn.Write([]byte(response))
				} else {
					// ECHO without argument - return error or empty bulk string
					conn.Write([]byte("$0\r\n\r\n"))
				}
			case "SET":
				if len(args) >= 3 {
					db.data[args[1]] = args[2]
					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("$0\r\n\r\n"))
				}
			case "GET":
				if len(args) >= 2 {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(db.data[args[1]]), db.data[args[1]])
					conn.Write([]byte(response))
				} else {
					conn.Write([]byte("$0\r\n\r\n"))
				}

			default:
				// Unknown command
				errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
				conn.Write([]byte(errorMsg))
			}
		}
	}
}
