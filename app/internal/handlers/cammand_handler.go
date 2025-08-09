package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
)

func writeError(conn net.Conn, err error) {
	errorMsg := fmt.Sprintf("-ERR%s\r\n", err.Error())
	conn.Write([]byte(errorMsg))
}

func handlePing(conn net.Conn, args []string, DB *db.DB) {
	if len(args) == 1 {
		conn.Write([]byte("+PONG\r\n"))
	} else {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		conn.Write([]byte(response))
	}
}

func handleEcho(conn net.Conn, args []string, DB *db.DB) {
	if len(args) >= 2 {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("$0\r\n\r\n"))
	}
}

func handleSet(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 3 {
		writeError(conn, fmt.Errorf("wrong number of arguments for 'SET' command"))
		return
	}
	key := args[1]
	value := args[2]
	var ttlMs int64 = 0
	if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
		var err error
		ttlMs, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			writeError(conn, fmt.Errorf("invalid PX argument"))
			return
		}
	}

	DB.Set(key, value, ttlMs)
	conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 2 {
		writeError(conn, fmt.Errorf("wrong number of arguments for 'GET' command"))
		return
	}

	key := args[1]
	if val, ok := DB.Get(key); ok {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("$-1\r\n"))
	}
}

func handleType(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 2 {
		writeError(conn, fmt.Errorf("wrong number of arguments for 'GET' command"))
		return
	}
	key := args[1]
	keyType := DB.GetType(key)
	response := fmt.Sprintf("+%s\r\n", keyType)
	conn.Write([]byte(response))
}

func handleXAdd(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 5 || len(args)%2 != 1 {
		writeError(conn, fmt.Errorf("wrong number of arguments for 'XADD' command"))
		return
	}
	key := args[1]
	id := args[2]

	fields := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		fields[args[i]] = args[i+1]
	}

	outPutID, err := DB.XAdd(key, id, fields)
	if err != nil {
		writeError(conn, err)
		return
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(outPutID), outPutID)
	conn.Write([]byte(response))
}

func handleXRange(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 4 {
		writeError(conn, fmt.Errorf("wrong number of arguments for 'XRANGE' command"))
		return
	}
	key := args[1]
	start := args[2]
	end := args[3]
	entries := DB.XRange(key, start, end)
	response := formatStreamEntries(entries)
	conn.Write([]byte(response))
}

func handleXRead(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD' command\r\n"))
		return
	}

	var blockTimeout int64 = -1
	streamsIndex := -1

	for i, arg := range args {
		if strings.ToUpper(arg) == "BLOCK" {
			if i+1 >= len(args) {
				conn.Write([]byte("-ERR BLOCK requires a timeout\r\n"))
				return
			}
			var err error
			blockTimeout, err = strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR timeout is not an integer or out of range of 64-bit integer\r\n"))
				return
			}
		} else if strings.ToUpper(arg) == "STREAMS" {
			streamsIndex = i
			break
		}
	}

	if streamsIndex == -1 || len(args) <= streamsIndex+2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD' command\r\n"))
		return
	}

	numStreams := (len(args) - (streamsIndex + 1)) / 2
	keys := args[streamsIndex+1 : streamsIndex+1+numStreams]
	IDs := args[streamsIndex+1+numStreams:]

	// Resolve the '$' ID to the last ID of each stream before the blocking loop
	for i, ID := range IDs {
		if ID == "$" {
			lastID := DB.GetLastID(keys[i])
			IDs[i] = lastID
		}
	}

	var allEntries []db.StreamReadEntry
	var hasNewEntries bool = false
	start := time.Now()

	for {
		allEntries = []db.StreamReadEntry{}
		hasNewEntries = false
		for i, key := range keys {
			ID := IDs[i]
			entries := DB.XREAD(key, ID)
			if len(entries) > 0 {
				allEntries = append(allEntries, db.StreamReadEntry{Key: key, Entries: entries})
				hasNewEntries = true
			}
		}

		if hasNewEntries {
			break
		}

		if blockTimeout == -1 {
			break
		}

		if blockTimeout > 0 && time.Since(start) >= time.Duration(blockTimeout)*time.Millisecond {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !hasNewEntries {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(allEntries)))

	for _, streamEntry := range allEntries {
		builder.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(streamEntry.Key), streamEntry.Key))
		builder.WriteString(formatStreamEntries(streamEntry.Entries))
	}
	conn.Write([]byte(builder.String()))
}

func handleINCR(conn net.Conn, args []string, DB *db.DB) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'ICNR' command\r\n"))
		return
	}
	key := args[1]
	value := DB.INCR(key)
	if value == -1 {
		writeError(conn, fmt.Errorf(" value is not an integer or out of range"))
		return
	}
	response := fmt.Sprintf(":%d\r\n", value)
	conn.Write([]byte(response))
}

func handleMulti(conn net.Conn, args []string, DB *db.DB) {
	conn.Write([]byte("+OK\r\n"))
}
func handleExec(conn net.Conn, args []string, DB *db.DB) {
	writeError(conn, fmt.Errorf(" EXEC without MULTI"))
}
