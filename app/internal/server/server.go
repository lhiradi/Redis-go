package server

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
	"github.com/codecrafters-io/redis-starter-go/app/internal/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

func Start(port, replicaof, dir, dbFileName string) {
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
	db.RDBFileDir = dir
	db.RDBFileName = dbFileName

	if err := db.ParseAndLoadRDBFile(); err != nil {
		fmt.Println("Failed to load RDB file:", err)
		os.Exit(1)
	}
	
	db.Mu.RLock()
	fmt.Printf("DEBUG: Keys in DB after RDB load: %v\n", db.Data)
	for k := range db.Data {
		fmt.Printf("DEBUG: Found key '%s' after RDB load\n", k)
	}
	db.Mu.RUnlock()

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
		// Create a single reader for the master connection.
		reader := bufio.NewReader(conn)

		// Pass the reader to the handshake function.
		if err := handlers.HandshakeWithMaster(conn, reader, port); err != nil {
			fmt.Println("Handshake with master failed:", err.Error())
			os.Exit(1)
		}
		// Pass the same reader to the connection handler.
		go handlers.HandleMasterConnection(conn, db, reader)
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
