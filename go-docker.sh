#!/usr/bin/env bash

docker run \
	-v $(pwd)/src/main/go:/source \
	-v $(pwd)/target/:/destination \
	-v $(pwd)/cache:/cache \
	-e "GOCACHE=/cache" \
	-e "CGO_ENABLED=0" \
	-w /source \
	golang:1.11 \
	/bin/bash -c "go build -a -installsuffix cgo -o /destination/server ./main"


docker build . -t hello-grpc-go


