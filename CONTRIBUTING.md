# Contributing to lakehouse-lite-engine

Thank you for contributing to `lakehouse-lite-engine`.

## Development Setup

Requirements:
- Python `>= 3.13, < 3.14`
- `uv` package manager

```bash
git clone https://github.com/Ali-datasmith/lakehouse-lite-engine.git
cd lakehouse-lite-engine
uv venv --python 3.13
uv sync
```

## Quality & Testing Commands

Before submitting code, ensure all checks pass:

```bash
# Linting & Formatting
uv run ruff check src tests
uv run ruff format --check src tests

# Static Type Checking
uv run mypy src

# Security Checks
uv run pip-audit
uv run bandit -r src

# Test Suite
uv run pytest
```

## Benchmark Execution

Run the real benchmark harness:

```bash
uv run python -m lakehouse_engine.benchmarks --rows 100000 --runs 2 --out ./bench-out
```

## Branching & Commit Policy

- Work on feature branches or `master` for production remediation.
- Use Conventional Commits format (e.g., `fix(catalog): enforce schema compatibility`).
- Ensure every commit is atomic and passes lint, type, and unit test checks.

## Security Reporting

Report security vulnerabilities privately to:
- Primary: `rajputmuhammadali979@gmail.com`
- Secondary: `rjptmhmmd@gmail.com`

See [SECURITY.md](SECURITY.md) for full policy.
