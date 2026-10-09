.DEFAULT_GOAL := help
.PHONY: help build vet lint test test-nocgo test-integration test-integration-compose bench bench-integration examples up down demo-setup demo-reset demo-check

COMPOSE_DSNS := postgres://shard:shard@localhost:5441/shard?sslmode=disable,postgres://shard:shard@localhost:5442/shard?sslmode=disable,postgres://shard:shard@localhost:5443/shard?sslmode=disable

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## Compile all packages
	go build ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

test: ## Run unit tests with the race detector
	go test -race ./...

test-nocgo: ## Run unit tests without cgo
	CGO_ENABLED=0 go test ./...

test-integration: ## Run integration tests (needs Docker; starts its own Postgres containers)
	go test -race -p 1 -tags integration ./...

# Same tests against the shards from `make up` instead of fresh containers.
# WARNING: wipes the public schema of those databases.
test-integration-compose: ## Run integration tests against `make up` shards
	SHARDTEST_DSNS='$(COMPOSE_DSNS)' go test -race -p 1 -tags integration ./...

BENCHTIME ?= 1s

bench: ## Run the benchmarks that need no database (BENCHTIME=1x for a quick check)
	go test -run '^$$' -bench . -benchmem -benchtime $(BENCHTIME) ./...

# Against the shards from `make up`. WARNING: wipes the public schema of those databases.
bench-integration: ## Run the benchmarks against `make up` shards
	SHARDTEST_DSNS='$(COMPOSE_DSNS)' go test -run '^$$' -bench . -benchmem -benchtime $(BENCHTIME) -p 1 -tags integration ./...

EXAMPLES := quickstart colocation fanout writes explain migrations observability adopt

examples: $(addprefix example-,$(EXAMPLES)) ## Run every example against the `make up` shards

example-%: ## Run one example (example-quickstart, ...) against the `make up` shards
	go run ./examples/$*

up: ## Start the 3 local Postgres shards and wait until healthy
	docker compose up -d --wait

down: ## Stop the local shards and remove their data
	docker compose down -v

demo-setup: up ## Start the shards, migrate them and load the demo dataset (docs/DEMO.md)
	go run ./examples/demo setup

demo-reset: up ## Return the shards to the demo's starting state
	go run ./examples/demo reset

# Runs the commands in docs/DEMO.md and compares their output with the guide.
# WARNING: resets the demo tables on the `make up` shards.
demo-check: demo-reset ## Run docs/DEMO.md and check its expected output
	DEMO_GUIDE=1 go test -count=1 -tags integration -run TestDemoGuide -v ./examples/demo
