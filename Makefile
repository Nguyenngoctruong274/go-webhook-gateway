.DEFAULT_GOAL := help
BIN := bin/webhookd
export WH_DATABASE_URL ?= postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable
TEST_DB := postgres://webhooks:webhooks@localhost:5432/webhooks_test?sslmode=disable

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the webhookd binary
	go build -trimpath -o $(BIN) ./cmd/webhookd

.PHONY: run
run: ## Run webhookd locally (needs `make up`)
	go run ./cmd/webhookd

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-db
test-db: ## Run all tests, including PostgreSQL ones (needs `make up`)
	WH_TEST_DATABASE_URL=$(TEST_DB) go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and print total coverage
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format code
	gofmt -s -w .

.PHONY: up
up: ## Start local dependencies (PostgreSQL)
	docker compose up -d

.PHONY: stack
stack: ## Run webhookd and PostgreSQL in Docker
	docker compose --profile stack up --build

.PHONY: down
down: ## Stop everything
	docker compose --profile stack down
