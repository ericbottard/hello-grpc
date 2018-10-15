package main

import (
	"github.com/fbiville/hello-grpc/src/main/go"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	listener, err := net.Listen("tcp", "localhost:9999")
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
