# Development Guide

This guide explains how to set up the project locally, build the application,
run tests, and perform common development tasks.

## Requirements

Install the following tools before starting:

- Rust: <https://www.rust-lang.org/tools/install>  
- Python 3.12: <https://www.python.org/downloads/>  
- uv: <https://github.com/astral-sh/uv>  

## Quick Start

Clone and build the project:

```bash
git clone git@github.com:CDLUC3/dmptool-works-matching.git
cd dmptool-works-matching
make install-release
make test
source .venv/bin/activate
```

`make install-release` creates the virtual environment (if needed) and installs
the package in editable mode with the Rust extension built in release mode.

## Development

### Create the virtual environment

The Makefile creates `.venv` using `uv` and installs `maturin`.

```bash
make venv
```

To activate the environment:

```bash
source .venv/bin/activate
```

### Building the Project

Setup Rust, including `rustfmt`, `clippy`, and `cargo-deny`:

```bash
make setup-rust
```

Build the Rust extension in debug mode and install the Python package in
editable mode with development extras:

```bash
make install
```

Build the Rust extension in release mode:

```bash
make install-release
```

Release mode enables compiler optimizations for Rust and is much faster for
compute-heavy Rust code.

The Python package is installed in **editable mode**, so changes to Python code
take effect immediately during development without reinstalling the package.

The project is compiled in release mode and installed via Python wheels in
`Dockerfile.aws`.

### CPU Optimization

By default, the Makefile compiles Rust with:

```bash
RUST_TARGET_CPU=native
```

This enables CPU-specific optimizations for the current machine.

You can override this if needed:

```bash
make install-release RUST_TARGET_CPU=x86-64
```

This can be useful for reproducible builds or matching CI environments.

## Running Tests

Run the Python test suite:

```bash
make test-python
```

Run SQLMesh tests:

```bash
make test-sqlmesh
```

The SQLMesh tests validate the transformation logic in the SQLMesh project:

```text
python/dmpworks/sql
```

Run all tests:

```bash
make test
```

## Linting and Formatting

The project uses the following tools:

- `ruff`: Python linting.
- `black`: Python formatting.
- `cargo fmt`: Rust formatting.
- `cargo clippy`: Rust linting.
- `cargo deny`: Rust dependency license checks.

Run formatting:

```bash
make fmt
```

Run all checks used by the project:

```bash
make lint
```

## Pre-commit

Run all checks and tests used by the project:

```bash
make pre-commit
```

## Cleaning the Environment

Remove the virtual environment and Rust build artifacts:

```bash
make clean
```

This deletes:

- `.venv`
- `target`

## Development Tools

### DuckDB

DuckDB is used to inspect the intermediate Parquet tables produced by the pipeline.
See [Running Locally](running-locally.md) for how to populate these tables.

Install DuckDB:

```bash
curl https://install.duckdb.org | DUCKDB_VERSION=1.4.4 sh
```

Open the DuckDB UI (requires `DATA_DIR` to be set — see
[Running Locally Section DATA_DIR](running-locally.md#12-data_dir)):

```bash
duckdb ${DATA_DIR}/duckdb/db.db -ui
```

Then open the UI in your browser: <http://localhost:4213>

You can also run a subset of SQLMesh models rather than the full pipeline. For
example, to load only the source dataset models:

```bash
sqlmesh -p python/dmpworks/sql plan \
    --select-model=crossref.crossref_metadata \
    --select-model=data_citation_corpus.relations \
    --select-model=openalex.openalex_works \
    --select-model=datacite.datacite
```

### Jupyter Lab

Jupyter Lab is installed automatically when the package is installed with dev
extras (i.e. `make install` or `make install-release`).

Start Jupyter Lab:

```bash
jupyter lab
```

Notebooks are in the `notebooks/` directory.

## Python Dependency Management

Python dependencies are managed with **uv**.

See what Python packages are installed vs the newest versions available:

```bash
uv pip list --outdated
```

Then edit dependency versions/ranges in `pyproject.toml` based on what
you want installed.

Then regenerate the lockfile and sync your environment:

```bash
uv lock --upgrade
uv sync --all-extras
```

## Rust Dependency Maintenance

Edit dependency requirements in `Cargo.toml`.

Update Rust dependencies to the newest versions allowed by `Cargo.toml`:

```bash
cargo update
```

Check dependency licenses and policies:

```bash
cargo deny check
```

Update `deny.toml` to maintain accepted licenses for `cargo-deny`.
