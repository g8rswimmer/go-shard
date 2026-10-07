# Contributing

Start with the design docs: [requirements](docs/REQUIREMENTS.md),
[architecture](docs/ARCHITECTURE.md) and [project plan](docs/PROJECT_PLAN.md).

## Requirements

- Go 1.26 or newer
- Docker (integration tests and the local shards)
- [golangci-lint](https://golangci-lint.run/) v1.64.8

## Everyday commands

```sh
make help              # list all targets
make test              # unit tests with the race detector (no Docker)
make up                # start 3 local Postgres shards (ports 5441-5443)
make test-integration  # integration tests (needs Docker)
make lint              # golangci-lint
make down              # stop the shards and delete their data
```

Set `POSTGRES_VERSION` to run the shards on another version (default 14, the
oldest supported):

```sh
POSTGRES_VERSION=18 make up
```

## Pull requests

- Work on a branch, one milestone step per PR.
- Add tests with the change: unit tests for logic, integration tests (build tag
  `integration`) for anything that talks to Postgres.
- `make vet lint test` must pass.
- Update the docs if a requirement or design decision changes.
