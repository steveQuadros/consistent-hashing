format:
	go fmt -n -x  ./...

vet:
	go vet ./...

test: format vet
	go test -race -cover ./...


