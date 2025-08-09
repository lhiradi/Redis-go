package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var port = flag.String("port", "6379", "Port for redis server")
var replicaOf = flag.String("replicaof", "", "Defines replica of master redis server")

func main() {
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()

	server.Start(*port, utils.ParsReplicaOf(*replicaOf))
}
