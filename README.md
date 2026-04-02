# dmptool-works-matching

DMP Tool Python package that matches DMPs to their published outputs.

## Requirements

* Python 3.12
* Rust: <https://www.rust-lang.org/tools/install>
* Docker Engine: <https://docs.docker.com/engine/install/>

## Data sources

* Crossref Metadata Public Data File: <https://www.crossref.org/learning/public-data-file/>
* OpenAlex: <https://docs.openalex.org/download-all-data/download-to-your-machine>
* DataCite Public Data File: <https://datafiles.datacite.org/>
* ROR: <https://zenodo.org/records/15132361>

## Documentation

See the below pages for in depth documentation.

* [Architecture](docs/architecture.md) — system overview, components, and data flow
* [Development](docs/development.md) — setting up a local development environment
* [Running Locally with Dataset Subset](docs/running-locally.md) — running the pipeline locally against a small dataset
* [AWS Operations](docs/aws/README.md) — deploying and operating the pipeline on AWS
* [Troubleshooting](docs/troubleshooting.md) — common issues and fixes
