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
	"WAIT":     handleWait,
	"CONFIG":   handleConfig,
	"KEYS":     handleKeys,
	"PUBLISH":  handlePublish,
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

func HandleMasterConnection(conn net.Conn, DB *db.DB, reader *bufio.Reader) {
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
			respCmdLength := len(utils.FormatRESPArray(args))

			response, _, err := handler(args, DB, activeTx)
			if err != nil {
				writeError(conn, err)
				fmt.Printf("Error handling command from master: %v\n", err)
				continue
			}
			if response != "" {
				conn.Write([]byte(response))
			}
			DB.UpdateOffset(respCmdLength)
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
	var inSubscribeMode bool
	clientSubscriptions := make(map[string]chan string)

	for {
		args := utils.ParseArgs(reader)
		if args == nil {
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		if inSubscribeMode {
			switch command {
			case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "QUIT", "RESET":
				//
			case "PING":
				response := "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
				conn.Write([]byte(response))
				continue
			default:
				errorMsg := fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", strings.ToLower(args[0]))
				conn.Write([]byte(errorMsg))
				continue
			}
		}
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
			fmt.Printf("Replica count after PSYNC: %d\n", len(DB.Replicas))
		} else if command == "SUBSCRIBE" {
			if len(args) < 2 {
				writeError(conn, fmt.Errorf(" wrong number of arguments for 'SUBSCRIBE' command"))
				continue
			}
			channel := args[1]
			if _, ok := clientSubscriptions[channel]; !ok {
				subChannel, _ := DB.PubSub.Subscribe(channel)
				clientSubscriptions[channel] = subChannel
				inSubscribeMode = true

				go func() {
					for msg := range subChannel {
						resp := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(msg), msg)
						conn.Write([]byte(resp))
					}
				}()
			}
			subscribersCount := len(clientSubscriptions)
			response := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subscribersCount)
			conn.Write([]byte(response))
		} else if command == "UNSUBSCRIBE" {
			if len(args) < 2 {
				writeError(conn, fmt.Errorf(" wrong number of arguments for 'UNSUBSCRIBE' command"))
				continue
			}
			channel := args[1]
			subChannel, ok := clientSubscriptions[channel]
			if ok {
				DB.PubSub.Unsubscribe(channel, subChannel)
				delete(clientSubscriptions, channel)
			}
			subscribersCount := len(clientSubscriptions)
			response := fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subscribersCount)
			conn.Write([]byte(response))

			if subscribersCount == 0 {
				inSubscribeMode = false
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
