package handlers

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type CmdHandler func(conn net.Conn, args []string, DB *db.DB)

// Map command strings to handler functions
var commandHandlers = map[string]CmdHandler{
	"PING":   handlePing,
	"ECHO":   handleEcho,
	"SET":    handleSet,
	"GET":    handleGet,
	"TYPE":   handleType,
	"XADD":   handleXAdd,
	"XRANGE": handleXRange,
	"XREAD":  handleXRead,
}

func HandleConnection(conn net.Conn, DB *db.DB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

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
			handler(conn, args, DB)
		} else {
			errorMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errorMsg))
		}
	}
}
