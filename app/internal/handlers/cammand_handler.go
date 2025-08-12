package handlers

import (
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
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

	if DB.Role == "master" {
		if len(args) == 1 {
			return "+PONG\r\n", nil, nil
		} else {
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			return response, nil, nil
		}
	}

	return "", nil, nil
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
		DB.UpdateOffset(len(utils.FormatRESPArray(args)))
		return "+OK\r\n", nil, nil
	}
	return "", nil, nil
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
		DB.UpdateOffset(len(utils.FormatRESPArray(args)))
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
		DB.UpdateOffset(len(utils.FormatRESPArray(args)))
		response := fmt.Sprintf(":%d\r\n", value)
		return response, nil, nil
	}
	return "", nil, nil

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

func handleWait(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		return "", activeTx, fmt.Errorf("WAIT command is not supported inside a transaction")
	}
	if len(args) < 3 {
		return "", nil, fmt.Errorf("wrong number of arguments for 'WAIT' command")
	}

	requiredAcks, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("error parsing numreplicas: %w", err)
	}

	timeOutMs, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("error parsing timeout: %w", err)
	}

	DB.ReplicaMu.RLock()
	numReplicas := int64(len(DB.Replicas))
	replicasToSignal := make([]*db.ReplicaConn, len(DB.Replicas))
	copy(replicasToSignal, DB.Replicas)
	DB.ReplicaMu.RUnlock()

	fmt.Printf("WAIT: Current replication offset: %d\n", DB.Offset)
	fmt.Printf("WAIT: Found %d replicas to signal.\n", numReplicas)

	if requiredAcks <= 0 || DB.Offset == 0 || numReplicas == 0 {
		return fmt.Sprintf(":%d\r\n", numReplicas), nil, nil
	}

	atomic.StoreInt64(&DB.NumAcksRecieved, 0)

	getAckCommand := []byte(utils.FormatRESPArray([]string{"REPLCONF", "GETACK", "*"}))
	fmt.Printf("WAIT: Preparing to send GETACK command: %s", string(getAckCommand))

	fmt.Printf("WAIT: Sending GETACK to %d replicas\n", len(replicasToSignal))
	sentCount := 0
	// This loop sends the command to ALL replicas.
	for i, rc := range replicasToSignal {
		rc.Mu.Lock()
		_, err := rc.Conn.Write(getAckCommand)
		rc.Mu.Unlock()
		if err != nil {
			fmt.Printf("WAIT: Failed to send GETACK to replica %d (%v): %v\n", i, rc.Conn.RemoteAddr(), err)
		} else {
			fmt.Printf("WAIT: Successfully sent GETACK to replica %d (%v)\n", i, rc.Conn.RemoteAddr())
			sentCount++
		}
	}

	// The waiting logic is now outside the sending loop.
	timeout := time.Duration(timeOutMs) * time.Millisecond
	timeoutChannel := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	fmt.Printf("WAIT: Starting wait loop for %d acks (from %d known replicas), timeout %v\n", requiredAcks, numReplicas, timeout)

	for {
		select {
		case <-ticker.C:
			currentAcks := atomic.LoadInt64(&DB.NumAcksRecieved)
			fmt.Printf("WAIT: Ticker check - Acks received: %d / %d\n", currentAcks, requiredAcks)
			if currentAcks >= requiredAcks {
				fmt.Printf("WAIT: Condition met (required): %d >= %d\n", currentAcks, requiredAcks)
				return fmt.Sprintf(":%d\r\n", currentAcks), nil, nil
			}
		case <-timeoutChannel:
			finalAcks := atomic.LoadInt64(&DB.NumAcksRecieved)
			fmt.Printf("WAIT: Timeout reached. Acks received: %d\n", finalAcks)
			return fmt.Sprintf(":%d\r\n", finalAcks), nil, nil
		}
	}
}

func handleConfig(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("CONFIG", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}

	if len(args) < 3 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'CONFIG' command")
	}

	if args[1] == "GET" {
		subCommand := strings.ToLower(args[2])
		switch subCommand {
		case "dir":
			response := utils.FormatRESPArray([]string{"dir", DB.RDBFileDir})
			return response, nil, nil

		case "dbfilename":
			response := utils.FormatRESPArray([]string{"dbfilename", DB.RDBFileName})
			return response, nil, nil
		}

		return "", nil, fmt.Errorf(" wrong arguments for 'CONFIG' command")
	}
	return "", nil, fmt.Errorf(" wrong arguments for 'CONFIG' command")
}

func handleKeys(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("KEYS", args[1:])
	}

	if len(args) < 2 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'KEYS' command")
	}

	pattern := args[1]
	DB.Mu.RLock()
	defer DB.Mu.RUnlock()

	var matchingKeys []string
	for key := range DB.Data {
		match, err := filepath.Match(pattern, key)
		if err != nil {
			return "", nil, err
		}
		if match {
			matchingKeys = append(matchingKeys, key)
		}
	}
	response := utils.FormatRESPArray(matchingKeys)
	return response, nil, nil
}

func handleSubscribe(args []string, DB *db.DB, activeTx *transaction.Transaction, subscribedChannels *[]string) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("SUBSCRIBE", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf(" wrong number of arguments for 'SUBSCRIBE' command")
	}
	channel := args[1]

	if !slices.Contains(*subscribedChannels, channel) {
		*subscribedChannels = append(*subscribedChannels, channel)
	}

	response := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(args[1]), args[1], len(*subscribedChannels))
	return response, nil, nil
}
