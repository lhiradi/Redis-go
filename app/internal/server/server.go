package server

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/handlers"
)

func Start(port, replicaof string) {
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Server listening on port", port)

	role := "master"
	if replicaof != "" {
		role = "slave"
	}
	db := db.New(role)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handlers.HandleConnection(conn, db)
	}
}
