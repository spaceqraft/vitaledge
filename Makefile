run: build
	./bin/vitaledge

build:
	go build -o bin/vitaledge ./cmd/vitaledge/main.go 

test:
	go test -v ./...

cover:
	go test -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt -tags ci,memoryprotection -race -timeout 15m -count=1 ./...
	sed -i '/mock_.*.go/d' coverage.txt # remove mock_.*.go files from test coverage
	go tool cover -html=coverage.txt -o coverage.html
