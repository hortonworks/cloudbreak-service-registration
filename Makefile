BINARY=service-registration

GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

deps:
	go get github.com/keyki/glu
	go get github.com/tools/godep

format:
	@gofmt -w ${GOFILES_NOVENDOR}

build: format build-darwin

build-darwin:
	GOOS=darwin CGO_ENABLED=0 go build -o ${BINARY}-darwin main.go

build-linux:
	GOOS=linux CGO_ENABLED=0 go build -o ${BINARY}-linux main.go

.DEFAULT_GOAL := build

.PHONY: build
