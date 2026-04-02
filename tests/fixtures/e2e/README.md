# E2E Test Fixtures

Build e2e test fixture data by extracting records matching a set of DOIs from upstream data dumps.

## Usage

```bash
python tests/fixtures/e2e/build_fixtures.py --env-file .env.local --doi-file tests/fixtures/e2e/dois.txt
```

### Prerequisites

Requires a `.env` file with the `UPSTREAM_*` path variables set. Copy `.env.local.example` to `.env.local` and fill in the upstream source paths (see the "Upstream Source Paths" section in that file).

### DOI file

The `--doi-file` argument takes a plain text file with one DOI per line. Lines starting with `#` are ignored. See `dois.txt` for the current set.

## How it works

1. **Pass 1** — Scan OpenAlex, Crossref, and DataCite dumps for DOI matches (parallel per-file). Collect matched records and extract ROR IDs from them.
2. **Pass 2** — Scan Data Citation Corpus for DOI matches (parallel per-file).
3. **Pass 3** — Filter the ROR dump by ROR IDs collected in pass 1 (single file, single-threaded).

Matched records are written as plain-text fixtures to `source/`:

```
source/
  openalex/works.jsonl
  crossref/metadata.jsonl
  datacite/datacite.jsonl
  data_citation_corpus/dcc.json
  ror/ror.json
```

## Rebuilding fixtures

To rebuild from scratch, delete the existing fixture files first:

```bash
rm -rf tests/fixtures/e2e/source/*/
python tests/fixtures/e2e/build_fixtures.py --env-file .env.local --doi-file tests/fixtures/e2e/dois.txt
```

Output defaults to `source/` in this directory. Use `--out-dir` to write elsewhere.
