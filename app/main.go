package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var port = flag.String("port", "6379", "Port for redis server")
var replicaOf = flag.String("replicaof", "", "Defines replica of master redis server")
var dir = flag.String("dir", "/tmp", "The path to the directory where the RDB file is stored")
var dbFileName = flag.String("dbfilename", "redis-data.rdb", "The name of the RDB file")

func main() {
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()

	server.Start(*port, *replicaOf, *dir, *dbFileName)
}
