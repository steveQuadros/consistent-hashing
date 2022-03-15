format:
	go fmt  ./...

vet:
	go vet ./...

test: format vet
	go test -v -race -cover ./...


