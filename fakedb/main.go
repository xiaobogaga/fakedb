package main

import (
	"context"
	"flag"
	"github.com/xiaobogaga/fakedb/prog"
	"github.com/xiaobogaga/fakedb/util"
	"time"
)

var (
	port          = flag.Int("port", 10010, "the port")
	server        = flag.Bool("server", true, "init server")
	client        = flag.Bool("client", false, "init client")
	filePath      = flag.String("p", "/tmp/", "the data and wal file path")
	flushDuration = flag.Int("f", 30, "flush duration at seconds")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	// *client = true
	if *client {
		prog.RunClient(*port)
		return
	}
	if *server {
		dataFile := *filePath + "fakedb.db"
		checkPointFile := *filePath + "fakedb.checkpoint"
		wal := *filePath + "fakedb.wal"
		logPath := *filePath + "fakedb.log"
		err := util.InitLogger(logPath, 1024*16, time.Second*10, true)
		if err != nil {
			panic(err)
		}
		prog.RunServer(ctx, *port, dataFile, checkPointFile, wal, time.Duration(*flushDuration)*time.Second)
	}
}
