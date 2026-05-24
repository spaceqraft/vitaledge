ANTLR_JAR := tools/antlr-4.13.1-complete.jar
TCK_VERSION := M23
TCK_CACHE_DIR := .cache/opencypher
TCK_ZIP := $(TCK_CACHE_DIR)/tck-$(TCK_VERSION).zip
TCK_ROOT := $(TCK_CACHE_DIR)/tck
TCK_FEATURES := $(TCK_ROOT)/features
TCK_FEATURES_ABS := $(abspath $(TCK_FEATURES))
CYPHER_COMPLIANCE_LOG := $(TCK_CACHE_DIR)/cypher-compliance.log

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

cypher-compliance-fetch:
	@mkdir -p $(TCK_CACHE_DIR)
	@rm -rf $(TCK_ROOT)
	@curl -L --fail https://s3.amazonaws.com/artifacts.opencypher.org/$(TCK_VERSION)/tck-$(TCK_VERSION).zip -o $(TCK_ZIP)
	@unzip -oq $(TCK_ZIP) -d $(TCK_CACHE_DIR)

cypher-compliance: cypher-compliance-fetch
	@VITALEDGE_CYPHER_TCK_DIR=$(TCK_FEATURES_ABS) go test -v ./internal/cypher/compliance -run TestCypherCompliance -count=1

cypher-compliance-report: cypher-compliance-fetch
	@VITALEDGE_CYPHER_TCK_DIR=$(TCK_FEATURES_ABS) go test -v ./internal/cypher/compliance -run TestCypherCompliance -count=1 > $(CYPHER_COMPLIANCE_LOG) 2>&1; \
	status=$$?; \
	bash scripts/summarize_cypher_compliance.sh $(CYPHER_COMPLIANCE_LOG); \
	exit $$status

cypher-compliance-summary:
	@bash scripts/summarize_cypher_compliance.sh $(CYPHER_COMPLIANCE_LOG)
