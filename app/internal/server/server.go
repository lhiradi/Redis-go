package server

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
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

	if role == "slave" {
		masterAddr := utils.ParsReplicaOf(replicaof)
		if masterAddr == "" {
			return
		}
		fmt.Printf("Connecting to master at %s...\n", masterAddr)

		conn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			fmt.Println("Failed to connect to master:", err.Error())
			os.Exit(1)
		}
		defer conn.Close()

		if err := handlers.HandshakeWithMaster(conn, port); err != nil {
			fmt.Println("Handshake with master failed:", err.Error())
			os.Exit(1)
		}
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handlers.HandleConnection(conn, db)
	}
}
