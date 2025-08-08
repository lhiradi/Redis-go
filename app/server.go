package main

import (
	"fmt"
	"net"
	"os"
)

func Start(port string) {
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Server listening on port", port)

	db := New()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn, db)
	}
}
