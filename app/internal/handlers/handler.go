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

// CmdHandler now takes a transaction object and returns the RESP string, the updated transaction and an error.
type CmdHandler func(args []string, DB *db.DB, activeTx *transaction.Transaction) (string, *transaction.Transaction, error)

// Map command strings to handler functions, updated for the new signature.
var commandHandlers = map[string]CmdHandler{
	"PING":     handlePing,
	"ECHO":     handleEcho,
	"SET":      handleSet,
	"GET":      handleGet,
	"TYPE":     handleType,
	"XADD":     handleXAdd,
	"XRANGE":   handleXRange,
	"INCR":     handleINCR,
	"MULTI":    handleMulti,
	"INFO":     handleInfo,
	"REPLCONF": handleReplconf,
}

func handleXReadWrapper(conn net.Conn, args []string, DB *db.DB, activeTx *transaction.Transaction) (*transaction.Transaction, error) {
	response, tx, err := handleXRead(args, DB, activeTx)
	if err != nil {
		writeError(conn, err)
		return tx, nil
	}
	conn.Write([]byte(response))
	return tx, nil
}

func HandleMasterConnection(conn net.Conn, DB *db.DB) {
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
			response, _, err := handler(args, DB, activeTx)
			if err != nil {
				// Write the error back to the master before continuing.
				writeError(conn, err)
				fmt.Printf("Error handling command from master: %v\n", err)
				continue
			}
			if response != "" {
				_, err := conn.Write([]byte(response))
				if err != nil {
					fmt.Printf("Failed to write response to master: %v\n", err)
					return
				}
			}
		} else {
			errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errorMsg))
			fmt.Printf("Unknown command from master: '%s'\n", args[0])
		}
	}
}

func HandleConnection(conn net.Conn, DB *db.DB) {
	defer conn.Close()
	defer DB.RemoveReplica(conn)
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

		if command == "EXEC" {
			response, newTx, err := handleExec(args, DB, activeTx, commandHandlers)
			activeTx = newTx
			if err != nil {
				writeError(conn, err)
				continue
			}
			conn.Write([]byte(response))
			continue
		} else if handler, ok := commandHandlers[command]; ok {
			// Check if we are in a transaction
			if activeTx != nil {
				activeTx.AddCommand(command, args[1:])
				conn.Write([]byte("+QUEUED\r\n"))
				continue
			}

			// Handle regular commands outside of a transaction or special commands like MULTI
			response, newTx, err := handler(args, DB, activeTx)
			if err != nil {
				writeError(conn, err)
				activeTx = nil // Reset transaction on error
				continue
			}
			activeTx = newTx
			conn.Write([]byte(response))

		} else if command == "XREAD" {
			// XREAD needs direct access to conn for blocking, so it's handled as a special case.
			activeTx, _ = handleXReadWrapper(conn, args, DB, activeTx)
		} else if command == "DISCARD" {
			response, newTx, err := handleDiscard(activeTx)
			activeTx = newTx
			if err != nil {
				writeError(conn, err)
			}
			conn.Write([]byte(response))
		} else if command == "PSYNC" {
			if err := handlePsync(conn, args, DB); err != nil {
				writeError(conn, err)
			}
		} else {
			errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errorMsg))
			if activeTx != nil {
				activeTx = nil
			}
		}
	}
}
