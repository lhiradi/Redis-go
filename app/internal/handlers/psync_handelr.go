package handlers

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
)

func handlePsync(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error) {
	if activeTx != nil {
		activeTx.AddCommand("PSYNC", args[1:])
		return "+QUEUED", activeTx, nil
	}
	if args[1] == "?" && args[2] == "-1" {
		return fmt.Sprintf("+FULLRESYNC %s %d\r\n", DB.ID, DB.Offset), nil, nil
	}
	return "", nil, fmt.Errorf(" not enough arguments")
}
