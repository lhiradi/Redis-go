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

func handlePing(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("PING", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) == 1 {
		return "+PONG\r\n", nil, nil
	} else {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		return response, nil, nil
	}
}

func handleEcho(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("ECHO", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) >= 2 {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		return response, nil, nil
	} else {
		return "$0\r\n\r\n", nil, nil
	}
}

func handleSet(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("SET", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 3 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'SET' command")
	}
	key := args[1]
	value := args[2]
	var ttlMs int64 = 0
	if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
		var err error
		ttlMs, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf(" invalid PX argument")
		}
	}

	DB.Set(key, value, ttlMs)
	if DB.Role == "master" {
		DB.PropagateCommand(args)
	}
	return "+OK\r\n", nil, nil
}

func handleGet(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("GET", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'GET' command")
	}

	key := args[1]
	if val, ok := DB.Get(key); ok {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
		return response, nil, nil
	} else {
		return "$-1\r\n", nil, nil
	}
}

func handleType(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("TYPE", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'GET' command")
	}
	key := args[1]
	keyType := DB.GetType(key)
	response := fmt.Sprintf("+%s\r\n", keyType)
	return response, nil, nil
}

func handleXAdd(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XADD", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 5 || len(args)%2 != 1 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'XADD' command")
	}
	key := args[1]
	id := args[2]

	fields := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		fields[args[i]] = args[i+1]
	}

	outPutID, err := DB.XAdd(key, id, fields)
	if err != nil {
		return "", nil, err
	}
	if DB.Role == "master" {
		DB.PropagateCommand(args)
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(outPutID), outPutID)
	return response, nil, nil
}

func handleXRange(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XRANGE", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 4 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'XRANGE' command")
	}
	key := args[1]
	start := args[2]
	end := args[3]
	entries := DB.XRange(key, start, end)
	response := formatStreamEntries(entries)
	return response, nil, nil
}

func handleXRead(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("XREAD", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'XREAD' command\r\n", nil, nil
	}

	var blockTimeout int64 = -1
	streamsIndex := -1

	for i, arg := range args {
		if strings.ToUpper(arg) == "BLOCK" {
			if i+1 >= len(args) {
				return "-ERR BLOCK requires a timeout\r\n", nil, nil
			}
			var err error
			blockTimeout, err = strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return "-ERR timeout is not an integer or out of range of 64-bit integer\r\n", nil, nil
			}
		} else if strings.ToUpper(arg) == "STREAMS" {
			streamsIndex = i
			break
		}
	}

	if streamsIndex == -1 || len(args) <= streamsIndex+2 {
		return "-ERR wrong number of arguments for 'XREAD' command\r\n", nil, nil
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
		return "$-1\r\n", nil, nil
	}

	response := formatXReadResponse(allEntries)
	return response, nil, nil
}

func handleINCR(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("INCR", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'INCR' command")
	}
	key := args[1]
	value := DB.INCR(key)
	if value == -1 {
		return "", nil, fmt.Errorf(" value is not an integer or out of range")
	}
	if DB.Role == "master" {
		DB.PropagateCommand(args)
	}
	response := fmt.Sprintf(":%d\r\n", value)
	return response, nil, nil
}

func handleMulti(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		return "", activeTx, fmt.Errorf(" MULTI is already active")
	}
	return "+OK\r\n", transaction.NewTransaction(), nil
}

func handleExec(args []string, DB *db.DB, activeTx *transaction.Transaction, commandHandlers map[string]CmdHandler) (string, *transaction.Transaction, error) {
	if activeTx == nil {
		return "", nil, fmt.Errorf(" EXEC without MULTI")
	}

	if len(activeTx.Commands) == 0 {
		return "*0\r\n", nil, nil
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(activeTx.Commands)))

	for _, command := range activeTx.Commands {
		handler, ok := commandHandlers[command.Name]
		if !ok {
			builder.WriteString(fmt.Sprintf("-ERR unknown command '%s'\r\n", command.Name))
			continue
		}
		// The activeTx is nil here because the nested commands are not part of another transaction
		response, _, err := handler(append([]string{command.Name}, command.Args...), DB, nil)
		if err != nil {
			builder.WriteString(fmt.Sprintf("-ERR%s\r\n", err.Error()))
		} else {
			builder.WriteString(response)
		}
	}
	return builder.String(), nil, nil
}

func handleDiscard(activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx == nil {
		return "", nil, fmt.Errorf(" DISCARD without MULTI")
	}

	return "+OK\r\n", nil, nil
}

func handleInfo(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	var infoBuilder strings.Builder

	infoBuilder.WriteString(fmt.Sprintf("role:%s\r\n", DB.Role))
	infoBuilder.WriteString(fmt.Sprintf("master_replid:%s\r\n", DB.ID))
	infoBuilder.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", DB.Offset))

	infoString := infoBuilder.String()

	return fmt.Sprintf("$%d\r\n%s\r\n", len(infoString), infoString), nil, nil
}