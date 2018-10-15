package main

import (
	"fmt"
	"github.com/fbiville/hello-grpc/src/main/go"
	"io"
	"time"
)

type AgeService struct {
}

func (*AgeService) GetAges(server hello.Hello_GetAgesServer) error {
	for true {
		person, err := server.Recv()
		if err == io.EOF {
			fmt.Println("EOF")
			return nil
		}
		if err != nil {
			fmt.Printf("Error %v\n", err)
			return err
		}
		fmt.Printf("Got %v\n", person)
		dob, err := time.Parse("2006-01-02", person.BirthDate)
		if err != nil {
			return err
		}
		server.Send(&hello.Reply{
			Name: person.Name,
			Age:  int64(time.Now().Sub(dob).Hours()) / 24 / 365,
		})
	}
	return nil
}
