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

## Dependency management

### Python dependencies
The uv virtual environment: initial install: `brew install uv`, updating: `brew upgrade uv`. If you have issues with uv, you can uninstall and reinstall it.

Upgrade all python dependencies: `uv sync --upgrade`
Upgrade a single python dependency: `uv sync --upgrade-package <package-name>`

### Rust dependencies
The rust toolchain uses Cargo to manage dependencies. To install Rust and Cargo see https://doc.rust-lang.org/cargo/getting-started/installation.html

Upgrade all Rust dependencies: `cargo update`

### Deploying after dependency updates
After updating dependencies, you will need to rebuild the Docker images and push them to ECR. Use the following commands to rebuild, push the image and update the SSM parameter that contains the latest Image tags. Then publish updates to CloudFormation
```
make deploy-[env]

cd infra && sceptre --var-file vars-[env].yaml launch [env]
```


## Documentation

See the below pages for in depth documentation.

* [Architecture](docs/architecture.md) — system overview, components, and data flow
* [Development](docs/development.md) — setting up a local development environment
* [Running Locally with Dataset Subset](docs/running-locally.md) — running the pipeline locally against a small dataset
* [AWS Operations](docs/aws/README.md) — deploying and operating the pipeline on AWS
* [Troubleshooting](docs/troubleshooting.md) — common issues and fixes
