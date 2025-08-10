package handlers

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
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
		if args[2] == "*" {
			response := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
			return response, nil, nil
		}

	}

	return "-ERR Unrecognized REPLCONF subcommand\r\n", nil, nil
}
