package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

func handleReplconf(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("REPLCONF", args[1:])
		return "+QUEUED\r\n", activeTx, nil
	}
	if len(args) < 3 {
		return "", nil, fmt.Errorf("wrong number of arguments for 'REPLCONF' command")
	}

	subcommand := strings.ToUpper(args[1])
	switch subcommand {
	case "LISTENING-PORT":
		return "+OK\r\n", nil, nil
	case "CAPA":
		return "+OK\r\n", nil, nil

	case "GETACK":
		if len(args) < 3 || args[2] != "*" {
			return "-ERR REPLCONF GETACK requires '*' as the second argument\r\n", nil, nil
		}
		response := utils.FormatRESPArray([]string{"REPLCONF", "ACK", strconv.Itoa(DB.Replication.Offset)})
		return response, nil, nil

	case "ACK":
		if len(args) < 3 {
			return "-ERR REPLCONF ACK requires an offset argument\r\n", nil, nil
		}
		atomic.AddInt64(&DB.Replication.NumAcksRecieved, 1)
		return "", nil, nil
	}

	return "-ERR Unrecognized REPLCONF subcommand\r\n", nil, nil
}