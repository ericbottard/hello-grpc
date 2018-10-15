#!/bin/bash
set -e

pushd src/main/go
protoc -I ../proto ../proto/hello.proto --go_out=plugins=grpc:.
go build -o server ./main
