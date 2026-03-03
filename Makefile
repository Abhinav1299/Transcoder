.PHONY: build test lint bench verify clean

GO ?= go

build:
	$(GO) build -o transcoder ./cmd/transcoder

test:
	$(GO) test -race -count=1 ./...

lint:
	$(GO) vet ./...

bench:
	$(GO) test -bench=. -benchmem -count=1 -run='^$$' ./...

verify: build
	@rm -rf /tmp/verify
	@mkdir -p /tmp/verify/text /tmp/verify/parquet
	@echo "--- Extracting text and parquet ---"
	@unzip -qo $(INPUT_ZIP) '*.log' -d /tmp/verify/text
	@./transcoder -input $(INPUT_ZIP) -output /tmp/verify/parquet.zip
	@unzip -qo /tmp/verify/parquet.zip '*.parquet' -d /tmp/verify/parquet
	@echo "--- Running DuckDB verification ---"
	@duckdb < scripts/verify.sql
	@rm -rf /tmp/verify

clean:
	rm -f transcoder transcoder_test_bin
	rm -rf outputs1 /tmp/verify
