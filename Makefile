run:
	go run .

test:
	go test -v ./...

coverage:
	go test -v -coverprofile=coverage.out ./...

openTestReport:coverage
	go tool cover -html=coverage.out

lint:
	golangci-lint run

format:
	go fmt ./...
