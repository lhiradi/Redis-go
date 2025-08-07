package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type DB struct {
	data map[string]cacheValue
	mu   sync.RWMutex
}

type cacheValue struct {
	value string
	ttl   int64 // Unix timestamp in seconds; 0 means no expiration
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.data[key]
	if !ok {
		return "", false
	}

	if val.ttl > 0 && time.Now().UnixMilli() > val.ttl {
		// Expired
		delete(db.data, key)
		return "", false
	}
	return val.value, true
}

func (db *DB) Set(key, value string, ttlMilSec int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var ttl int64
	if ttlMilSec > 0 {
		ttl = time.Now().UnixMilli() + ttlMilSec
	}
	db.data[key] = cacheValue{value: value, ttl: ttl}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	db := &DB{
		data: make(map[string]cacheValue),
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *DB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args := getArgs(reader)
		if args == nil {
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		switch command {
		case "PING":
			if len(args) == 1 {
				conn.Write([]byte("+PONG\r\n"))
			} else {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
				conn.Write([]byte(response))
			}
		case "ECHO":
			if len(args) >= 2 {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("$0\r\n\r\n"))
			}
		case "SET":
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
				continue
			}
			key := args[1]
			value := args[2]
			var ttlMs int64 = 0
			if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
				var err error
				ttlMs, err = strconv.ParseInt(args[4], 10, 64)
				if err != nil {
					conn.Write([]byte("-ERR invalid PX argument\r\n"))
					continue
				}
			}

			db.Set(key, value, ttlMs)
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
				continue
			}

			key := args[1]
			if val, ok := db.Get(key); ok {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}

		default:
			errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errorMsg))
		}
	}
}

func getArgs(reader *bufio.Reader) []string {
	line, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Print(err)
		return nil
	}

	if len(line) < 2 || line[0] != '*' {
		log.Printf("Invalid RESP format: %s", line)
		return nil
	}

	var arrayLength int
	_, err = fmt.Sscanf(line, "*%d\r\n", &arrayLength)
	if err != nil {
		log.Printf("Failed to parse array length: %v", err)
		return nil
	}

	args := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			log.Print(err)
			return nil
		}

		if len(lengthLine) < 2 || lengthLine[0] != '$' {
			log.Printf("Invalid bulk string format: %s", lengthLine)
			return nil
		}

		var strLen int
		_, err = fmt.Sscanf(lengthLine, "$%d\r\n", &strLen)
		if err != nil {
			log.Printf("Failed to parse string length: %v", err)
			return nil
		}

		arg := make([]byte, strLen+2)
		_, err = io.ReadFull(reader, arg)
		if err != nil {
			log.Print(err)
			return nil
		}

		args[i] = string(arg[:strLen])
	}
	return args
}
