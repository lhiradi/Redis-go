package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
)

func writeError(conn net.Conn, err error) {
	errorMsg := fmt.Sprintf("-ERR %s\r\n", err.Error())
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
	streamsIndex := -1
	for i, arg := range args {
		if strings.ToUpper(arg) == "STREAMS" {
			streamsIndex = i
			break
		}
	}

	if streamsIndex == -1 || len(args) <= streamsIndex+2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD' command\r\n"))
		return
	}

	keys := args[streamsIndex+1 : (len(args)+streamsIndex+1)/2]
	IDs := args[(len(args)+streamsIndex+1)/2:]

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(keys)))

	for i, key := range keys {
		ID := IDs[i]
		entries := DB.XREAD(key, ID)
		builder.WriteString(formatXReadEntries(key, entries))
	}
	conn.Write([]byte(builder.String()))

}
