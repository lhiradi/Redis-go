package handlers

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/transaction"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

// CmdHandler now takes a transaction object and returns the potentially updated transaction and an error.
type CmdHandler func(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error)

// Map command strings to handler functions, updated for the new signature.
var commandHandlers = map[string]CmdHandler{
	"PING":   handlePing,
	"ECHO":   handleEcho,
	"SET":    handleSet,
	"GET":    handleGet,
	"TYPE":   handleType,
	"XADD":   handleXAdd,
	"XRANGE": handleXRange,
	"XREAD":  handleXRead,
	"INCR":   handleINCR,
	"MULTI":  handleMulti,
	"EXEC":   handleExec,
}

func HandleConnection(conn net.Conn, DB *db.DB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var activeTx *transaction.Transaction

	for {
		args := utils.ParseArgs(reader)
		if args == nil {
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		if handler, ok := commandHandlers[command]; ok {
			var err error
			// Pass the current transaction state and update it with the return value
			activeTx, err = handler(conn, args, DB, activeTx)
			if err != nil {
				writeError(conn, err)
			}
		} else {
			errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errorMsg))
		}
	}
}
