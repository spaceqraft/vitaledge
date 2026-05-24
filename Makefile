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

bench-graph-store:
	go test ./internal/graph/store/pebble -run '^$$' -bench 'BenchmarkEdgeMutation' -benchmem -benchtime=200ms

cover:
	go test -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt -tags ci,memoryprotection -race -timeout 15m -count=1 ./...
	sed -i '/mock_.*.go/d' coverage.txt # remove mock_.*.go files from test coverage
	go tool cover -html=coverage.txt -o coverage.html

update-comprehension:
	python3 scripts/update_comprehension_docs_llm.py

verify-comprehension:
	@q_count=$$(grep -c '^### Q-[0-9]\{3\}:' COMPREHENSION-Q.md); \
	a_count=$$(grep -c '^### A-[0-9]\{3\}$$' COMPREHENSION-A.md); \
	if [ "$$q_count" -ne "$$a_count" ]; then \
		echo "ERROR: Q/A count mismatch (Q=$$q_count, A=$$a_count)"; \
		exit 1; \
	fi; \
	echo "Comprehension docs verified: Q=$$q_count A=$$a_count"
