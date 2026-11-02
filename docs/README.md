# Documentation index

Find the document by the question you have.

**Using the library**

| Question | Read |
|---|---|
| What does it do, and how do I start? | [../README.md](../README.md) |
| Which statements are routed, merged or refused? | [QUERIES.md](QUERIES.md) |
| I got an error. What does it mean? | [ERRORS.md](ERRORS.md) |
| How do I run it: timeouts, failures, retries, capacity? | [OPERATIONS.md](OPERATIONS.md) |
| How do I move an existing database into shards? | [ADOPTING.md](ADOPTING.md) |
| How do I see it work by hand? | [DEMO.md](DEMO.md) |
| How do I build it (cgo, cross-compiling, Docker)? | [BUILDING.md](BUILDING.md) |
| What does it cost? | [BENCHMARKS.md](BENCHMARKS.md) |
| What counts as a breaking change, and how do I upgrade? | [VERSIONING.md](VERSIONING.md), [UPGRADING.md](UPGRADING.md), [../CHANGELOG.md](../CHANGELOG.md) |
| Runnable examples? | [../examples](../examples/README.md) |

**Changing the library**

| Question | Read |
|---|---|
| What must the library do? (FR-1 to FR-14) | [REQUIREMENTS.md](REQUIREMENTS.md) |
| How is it built? Packages, interfaces, which test covers which requirement | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Is this already decided? | [DECISIONS.md](DECISIONS.md) |
| How do I make a common change (a route, a merge function, an error, a hook event)? | [CHANGING.md](CHANGING.md) |
| What was the plan, and what is open? | [PROJECT_PLAN.md](PROJECT_PLAN.md) |
| How do I run the tests, and what must pass? | [../CONTRIBUTING.md](../CONTRIBUTING.md), [../AGENTS.md](../AGENTS.md) |
| How do I report a security problem? | [../SECURITY.md](../SECURITY.md) |

If a document contradicts the code, the code is right and the document is a
bug: fix it in the same change.
