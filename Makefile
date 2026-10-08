.DEFAULT_GOAL := help
.PHONY: help build vet lint test test-nocgo test-integration test-integration-compose up down

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

up: ## Start the 3 local Postgres shards and wait until healthy
	docker compose up -d --wait

down: ## Stop the local shards and remove their data
	docker compose down -v
