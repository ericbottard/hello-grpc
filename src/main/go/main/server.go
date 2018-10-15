package main

import (
	"fmt"
	"github.com/fbiville/hello-grpc/src/main/go"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	port := "9999"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	fmt.Printf("starting at port %d", port)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}

	server := grpc.NewServer()
	service := AgeService{}
	hello.RegisterHelloServer(server, &service)

	server.Serve(listener)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		<-signals
		server.GracefulStop()
	}()
}
