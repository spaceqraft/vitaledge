ANTLR_JAR := tools/antlr-4.13.1-complete.jar

run: build
	./bin/vitaledge

build:
	go build -o bin/vitaledge ./cmd/vitaledge/main.go 
	go build -o bin/vitaledge-bench ./cmd/vitaledge-bench/main.go

generate-cypher-parser:
	@test -f $(ANTLR_JAR) || curl -L --fail https://www.antlr.org/download/antlr-4.13.1-complete.jar -o $(ANTLR_JAR)
	java -jar $(ANTLR_JAR) -Dlanguage=Go -visitor -listener -package cyphergen -Xexact-output-dir -o internal/cypher/grammar/generated internal/cypher/grammar/Cypher.g4

generate: generate-cypher-parser

test:
	go test -v ./...

bench-smoke:
	bash benchmarks/smoke.sh

cover:
	go test -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt -tags ci,memoryprotection -race -timeout 15m -count=1 ./...
	sed -i '/mock_.*.go/d' coverage.txt # remove mock_.*.go files from test coverage
	go tool cover -html=coverage.txt -o coverage.html
