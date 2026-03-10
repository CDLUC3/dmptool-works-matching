.RECIPEPREFIX := >

SHELL=/bin/bash

# Default target CPU for local development
RUST_TARGET_CPU ?= native
PYTHON_VERSION ?= 3.12

# Ensure uv is installed
check-uv:
> @command -v uv >/dev/null 2>&1 || { echo >&2 "uv is required but not installed. Aborting."; exit 1; }

venv: check-uv
> @if [ ! -d ".venv" ]; then \
>     uv venv --seed .venv; \
> fi
> @uv tool install maturin

install: venv
> uv sync --extra dev
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin develop --extras dev

setup-rust:
> rustup component add rustfmt clippy
> @command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny

install-release: venv
> uv sync --extra dev
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin develop --release --extras dev

# Build wheels for distribution/production
build-prod:
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin build --interpreter python$(PYTHON_VERSION) --release

fmt:
> cargo fmt --all
> .venv/bin/black ./python/dmpworks

lint:
> cargo clippy --all-features
> cargo deny check
> .venv/bin/ruff check ./python/dmpworks --fix

test-python:
> .venv/bin/pytest

test-sqlmesh:
> .venv/bin/sqlmesh -p ./python/dmpworks/sql/ test

pre-commit: venv fmt lint test test-sqlmesh

tests: test-python test-sqlmesh

clean:
> rm -rf .venv target