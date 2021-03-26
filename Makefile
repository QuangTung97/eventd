.PHONY: test lint install-tools

test:
	go test ./...

lint:
	go fmt ./...
	golint ./...
	go vet ./...
	errcheck ./...
	gocyclo -over 10 .

install-tools:
	go get github.com/golang/mock/mockgen
	go install golang.org/x/lint/golint
	go install github.com/kisielk/errcheck
	go get github.com/fzipp/gocyclo/cmd/gocyclo
