.DEFAULT_GOAL := help
.PHONY: help build vet lint test test-integration up down

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

test-integration: ## Run integration tests (needs Docker)
	go test -race -tags integration ./...

up: ## Start the 3 local Postgres shards and wait until healthy
	docker compose up -d --wait

down: ## Stop the local shards and remove their data
	docker compose down -v
