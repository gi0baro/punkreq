.DEFAULT_GOAL := all
pysources = punkreq tests

.PHONY: format
format:
	uv run ruff check --fix $(pysources)
	uv run ruff format $(pysources)

.PHONY: lint
lint:
	uv run ruff check $(pysources)
	uv run ruff format --check $(pysources)

.PHONY: test
test:
	uv run pytest -v tests

.PHONY: all
all: format lint test
