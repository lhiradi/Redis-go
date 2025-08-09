package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
)

func writeError(conn net.Conn, err error) {
	errorMsg := fmt.Sprintf("-ERR%s\r\n", err.Error())
	conn.Write([]byte(errorMsg))
}

func handlePing(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("PING", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) == 1 {
		conn.Write([]byte("+PONG\r\n"))
	} else {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		conn.Write([]byte(response))
	}
	return nil, nil
}

func handleEcho(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("ECHO", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) >= 2 {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("$0\r\n\r\n"))
	}
	return nil, nil
}

func handleSet(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("SET", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("wrong number of arguments for 'SET' command")
	}
	key := args[1]
	value := args[2]
	var ttlMs int64 = 0
	if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
		var err error
		ttlMs, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid PX argument")
		}
	}

	DB.Set(key, value, ttlMs)
	conn.Write([]byte("+OK\r\n"))
	return nil, nil
}

func handleGet(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("GET", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments for 'GET' command")
	}

	key := args[1]
	if val, ok := DB.Get(key); ok {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("$-1\r\n"))
	}
	return nil, nil
}

func handleType(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("TYPE", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments for 'GET' command")
	}
	key := args[1]
	keyType := DB.GetType(key)
	response := fmt.Sprintf("+%s\r\n", keyType)
	conn.Write([]byte(response))
	return nil, nil
}

func handleXAdd(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XADD", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 5 || len(args)%2 != 1 {
		return nil, fmt.Errorf("wrong number of arguments for 'XADD' command")
	}
	key := args[1]
	id := args[2]

	fields := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		fields[args[i]] = args[i+1]
	}

	outPutID, err := DB.XAdd(key, id, fields)
	if err != nil {
		return nil, err
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(outPutID), outPutID)
	conn.Write([]byte(response))
	return nil, nil
}

func handleXRange(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XRANGE", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 4 {
		return nil, fmt.Errorf("wrong number of arguments for 'XRANGE' command")
	}
	key := args[1]
	start := args[2]
	end := args[3]
	entries := DB.XRange(key, start, end)
	response := formatStreamEntries(entries)
	conn.Write([]byte(response))
	return nil, nil
}

func handleXRead(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XREAD", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD' command\r\n"))
		return nil, nil
	}

	var blockTimeout int64 = -1
	streamsIndex := -1

	for i, arg := range args {
		if strings.ToUpper(arg) == "BLOCK" {
			if i+1 >= len(args) {
				conn.Write([]byte("-ERR BLOCK requires a timeout\r\n"))
				return nil, nil
			}
			var err error
			blockTimeout, err = strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				conn.Write([]byte("-ERR timeout is not an integer or out of range of 64-bit integer\r\n"))
				return nil, nil
			}
		} else if strings.ToUpper(arg) == "STREAMS" {
			streamsIndex = i
			break
		}
	}

	if streamsIndex == -1 || len(args) <= streamsIndex+2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'XREAD' command\r\n"))
		return nil, nil
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
		return nil, nil
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(allEntries)))

	for _, streamEntry := range allEntries {
		builder.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(streamEntry.Key), streamEntry.Key))
		builder.WriteString(formatStreamEntries(streamEntry.Entries))
	}
	conn.Write([]byte(builder.String()))
	return nil, nil
}

func handleINCR(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("INCR", args[1:])
		conn.Write([]byte("+QUEUED\r\n"))
		return activeTx, nil
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments for 'ICNR' command")
	}
	key := args[1]
	value := DB.INCR(key)
	if value == -1 {
		return nil, fmt.Errorf("value is not an integer or out of range")
	}
	response := fmt.Sprintf(":%d\r\n", value)
	conn.Write([]byte(response))
	return nil, nil
}

func handleMulti(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx != nil {
		return activeTx, fmt.Errorf("MULTI is already active")
	}
	conn.Write([]byte("+OK\r\n"))
	return transaction.NewTransaction(), nil
}

func handleExec(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	if activeTx == nil {
		return nil, fmt.Errorf("EXEC without MULTI")
	}

	if len(activeTx.Commands) == 0 {
		conn.Write([]byte("*0\r\n")) // Returns an empty array for empty transactions
		return nil, nil
	}

	// This is where you would add the logic to execute all commands in the activeTx.Commands slice.
	// For this example, we will just return a simple response and reset the transaction state.
	// You need to replace this with your actual execution logic.
	conn.Write([]byte("+OK\r\n"))
	return nil, nil
}
