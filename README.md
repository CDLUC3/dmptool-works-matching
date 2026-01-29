# dmptool-works-matching
DMP Tool Python package that matches DMPs to potentially related works.

Requirements:
* Python 3.12
* Rust: https://www.rust-lang.org/tools/install
* Docker Engine: https://docs.docker.com/engine/install/

Data sources:
* Crossref Metadata Public Data File: https://www.crossref.org/learning/public-data-file/
* OpenAlex: https://docs.openalex.org/download-all-data/download-to-your-machine
* DataCite Public Data File: https://datafiles.datacite.org/
* ROR: https://zenodo.org/records/15132361

## Installation
Clone dmsp_api_prototype repo:
```bash
git clone git@github.com:CDLUC3/dmptool-works-matching.git
```

Clone Polars:
```bash
git clone fix-load-json-as-string --single-branch git@github.com:jdddog/polars.git
```

Clone pyo3 Polars:
```bash
git clone --branch local-build --single-branch git@github.com:jdddog/pyo3-polars.git
```

Make a Python virtual environment:
```bash
python -m venv polars/.venv
```

Activate the Python virtual environment:
```bash
source polars/.venv/activate
```

Install Polars dependencies:
```bash
(cd polars && rustup toolchain install nightly --component miri)
(cd polars/py-polars && make requirements-all)
```

Build Polars:
```bash
(cd polars/py-polars && RUSTFLAGS="-C target-cpu=native" make build-dist-release)
```

Install dmpworks Python package dependencies:
```bash
(cd dmptool-works-matching && pip install -e .[dev])
```

Build and install the dmpworks Python package, including its Polars expression 
plugin:
```bash
(cd dmsp_api_prototype/queries/dmpworks && RUSTFLAGS="-C target-cpu=native" maturin develop --release)
```

## Environment
Create an `.env.local` file:
```commandline
export SOURCES=/path/to/sources
export DATA=/path/to/data
export DMPWORKS=/path/to/dmsp_api_prototype/queries/dmpworks
export SQLMESH__GATEWAYS__DUCKDB__CONNECTION__DATABASE=/path/to/duckdb/db.db
export SQLMESH__VARIABLES__DATA_PATH=/path/to/data/transform/
export SQLMESH__VARIABLES__EXPORT_PATH=/path/to/data/export/
export SQLMESH__VARIABLES__AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD=1
export SQLMESH__VARIABLES__AUDIT_DATACITE_WORKS_THRESHOLD=1
export SQLMESH__VARIABLES__AUDIT_OPENALEX_WORKS_THRESHOLD=1
export SQLMESH__VARIABLES__CROSSREF_METADATA_PATH=/path/to/crossref_metadata/parquets
export SQLMESH__VARIABLES__DATACITE_PATH=/path/to/datacite/parquets
export SQLMESH__VARIABLES__OPENALEX_FUNDERS_PATH=/path/to/openalex_funders/parquets
export SQLMESH__VARIABLES__OPENALEX_WORKS_PATH=/path/to/openalex_works/parquets
export SQLMESH__VARIABLES__ROR_PATH=/path/to/ror/parquets
```

Source environment variables:
```bash
source .env.local
```

Create a demo version of the source datasets with works from UC Berkley:
```bash
./bin/demo_dataset.sh
```

Running OpenSearch locally:
```bash
docker compose up
```

Running Python tests:
```bash
pytest
```

## Transform Source Datasets
Raw datasets are first cleaned and normalised. Crossref Metadata, DataCite and
OpenAlex Works are separated into individual tables for works, authors, 
affiliations, funders and relations.

Transformations are performed using [Polars](https://pola.rs), a fast,
multi-threaded DataFrame library. Polars provides a consistent transformation
syntax and supports high-performance custom transformations written in Rust.

Identifiers are extracted from OpenAlex Funders and ROR to support SQL Mesh 
transformations that unify various identifier types (e.g. GRID, ISNI) into ROR 
and Crossref Funder Ids.

The processed data is stored into Parquet format, which is optimised for 
columnar databases such as DuckDB.

Common transformation steps include:
* Removing HTML markup from titles and abstracts; convert empty strings to null.
* Standardising date formats.
* Normalising identifiers, for example, by stripping URL prefixes.

DataCite specific transformations:
* Fixing inconsistencies in `affiliation` and `nameIdentifiers` schemas, which
can be lists or a single object.
* Extracting ORCID IDs from `nameIdentifiers`.

OpenAlex Works:
* Un-invert inverted abstract, e.g. `{"Hello":[0],"World":[1]}` to `Hello World`.

### Commands
Run the following commands to convert the source datasets into Parquet files.
The full output directory must already exist before you run the commands.

Create output directories:
```bash
mkdir -p "${DATA}/transform"/{datacite,openalex_works,crossref_metadata,openalex_funders,ror}
```

Crossref Metadata:
```bash
dmpworks transform crossref-metadata ${DATA}/sources/crossref_metadata ${DATA}/transform/crossref_metadata
```

OpenAlex Works:
```bash
dmpworks transform openalex-works ${DATA}/sources/openalex_works ${DATA}/transform/openalex_works
```

OpenAlex Funders:
```bash
dmpworks transform openalex-funders ${DATA}/sources/openalex_funders ${DATA}/transform/openalex_funders
```

DataCite:
```bash
dmpworks transform datacite ${DATA}/sources/datacite ${DATA}/transform/datacite
```

ROR:
```bash
dmpworks transform ror ${DATA}/sources/ror/v1.63-2025-04-03-ror-data_schema_v2.json ${DATA}/transform/ror
```

## Create Works Index Table
A unified "Works Index" is created by joining transformed source datasets
together. Each item contains a DOI, title, abstract, publication date, updated
date, affiliation names, affiliation ROR IDs, author names, author ORCID IDs,
funder names, award IDs and funder IDs.

The works index is created with [SQL Mesh](https://sqlmesh.readthedocs.io/en/latest/)
and [DuckDB](https://duckdb.org). SQLMesh is a tool for writing SQL data 
transformations and DuckDB is an embedded SQL database.

The works index consists of all works from DataCite, and works from OpenAlex
with DOIs that are not found in DataCite.

The transformations specific to DataCite include:
* Supplement records with OpenAlex metadata.
* Unify various identifier types (e.g. GRID, ISNI) into ROR and Crossref Funder 
Ids.
* Standardise work types.

The transformations specific OpenAlex include:
* Handling duplicate DOIs: different OpenAlex Works have the same DOI.
* Supplementing records with information from Crossref Metadata, including
titles and abstracts and funding information.

The final model `works_index.exports` exports the works index to Parquet.

### Commands
Run unit tests:
```bash
dmpworks sqlmesh test
```

Run SQL Mesh:
```bash
dmpworks sqlmesh plan
```

Run the DuckDB UI:
```bash
duckdb ${SQLMESH__GATEWAYS__DUCKDB__CONNECTION__DATABASE} -ui
```

To view the DuckDB database: http://localhost:4213.

## Create OpenSearch Indexes
[OpenSearch](https://opensearch.org) is used to match related works to
Data Management Plans.

Create the OpenSearch works index:
```bash
dmpworks opensearch create-index works-demo works-mapping.json
```

Sync the works index export with the OpenSearch works index:
```bash
dmpworks opensearch sync-works works-demo ${DATA}/export
```

Go to OpenSearch Dashboards to view the works index: http://localhost:5601.

## Learning to Rank
OpenSearch Learning to Rank is used to re-rank search results using a machine
learning model trained on a ground truth dataset of DMP-to-published-work
matches.

See here for an in-depth guide to Learning to Rank in the AWS OpenSearch Service:
https://docs.aws.amazon.com/opensearch-service/latest/developerguide/learning-to-rank.html

### Pre-requisites
The following steps must be completed before working with Learning to Rank.

In the OpenSearch console, initialise the Learning to Rank plugin:
```bash
PUT _ltr
```

Create a ground truth data file named `ground-truth-YYYY-MM-DD.csv` (see below for an example):
```csv
dmpDoi,workDoi,status
10.48321/d17598,10.1093/gigascience/giad004,ACCEPTED
10.48321/d17598,10.48448/vwk3-ej62,ACCEPTED
10.48321/d17598,10.5281/zenodo.10381316,ACCEPTED
10.48321/d17598,10.5281/zenodo.10381317,ACCEPTED
10.48321/d17598,10.5281/zenodo.12571687,ACCEPTED
```

Download `RankLib-2.18.jar` from:<br>
https://sourceforge.net/projects/lemur/files/lemur/RankLib-2.18/

Create a `features.txt` file that instructs RankLib what features from the
feature set to use for training:
```text
1
2
4
6
7
9
10
12
13
```

### Feature Set
The following is a summary of the Learning to Rank feature set used for training:

1. **mlt_content**: the [More Like This](https://docs.opensearch.org/latest/query-dsl/specialized/more-like-this/) score calculated between the titles and abstracts of the DMP and the work.
1. **funded_doi_matched**: whether a known funded DOI matched.
1. **dmp_award_count**: the number of awards that the DMP has.
1. **award_match_count**: the number of DMP awards that matched with the work.
1. **dmp_author_count**: the number of DMP authors.
1. **author_orcid_match_count**: the number of DMP ORCID IDs that matched with the work.
1. **author_surname_match_count**: the number of DMP author surnames that matched with the work.
1. **dmp_institution_count**: the number of DMP institutions.
1. **institution_ror_match_count**: the number of DMP ROR IDs that matched with the work.
1. **institution_name_match_count**: the number of DMP institution names that matched with the work.
1. **dmp_funder_count**: the number of DMP funders.
1. **funder_ror_match_count**: the number of DMP funder ROR IDs that matched with the work.
1. **funder_name_match_count**: the number of DMP funder names that matched with the work.

The feature set is defined in the `build_featureset` function in:<br>
[learning_to_rank.py](python/dmpworks/opensearch/learning_to_rank.py).

### Commands
Create the feature set in OpenSearch:
```bash
# 
dmpworks opensearch create-featureset \
                    dmpworks
```

Args:
* `dmpworks`: the OpenSearch Learning to Rank feature set name.

Generate the training dataset and save in RankLib format:
```bash
dmpworks opensearch generate-training-dataset \
                    ground-truth-2025-12-22.csv \
                    dmps-index \
                    works-index \
                    train-2025-12-22.txt \
                    dmpworks \
                    --query-builder-name=build_dmp_works_search_baseline_query \
                    --max-results=1000 
```

Args:
* `ground-truth-2025-12-22.csv`: the path to the ground truth data file.
* `dmps-index`: the OpenSearch DMP index name.
* `works-index`: the OpenSearch works index name.
* `train-2025-12-22.txt`: the path to the file where the training dataset will be saved.
* `dmpworks`: the featureset name

Options:
* `--query-builder-name=build_dmp_works_search_baseline_query`: which query builder to use.
* `--max-results=1000 `: the maximum number of works to include for each DMP.

Train a Learning to Rank model:
```bash
java -jar RankLib-2.18.jar \
          -train train-2025-12-22.txt \
          -feature features.txt \
          -ranker 4 \
          -metric2t MAP@10 \
          -norm zscore \
          -reg 0.01 \
          -save model-coordinate-ascent-2025-12-22.txt
```

Options:
* `-train train-2025-12-22.txt`: the path to the training dataset file.
* `-feature features.txt`: the path to the features.txt file which contains the subset of features to train.
* `-ranker 4`: what ranking model to train, in this case, Coordinate Ascent.
* `-metric2t MAP@10`: what metric to use during training.
* `-norm zscore`: the type of normalisation to use.
* `-reg 0.01`: regularization parameter.
* `-save model-coordinate-ascent-2025-12-22.txt`: the path to the file where the model will be saved.

Upload the Learning to Rank model to OpenSearch. At this step, the mean and standard 
deviation are computed for each feature (to match Z-score model normalisation) 
and supplied as feature normalisation data in the uploaded OpenSearch Learning 
to Rank model.
```bash
dmpworks opensearch upload-ranklib-model \
                    dmpworks \
                    model-coordinate-ascent-2025-12-22 \
                    model-coordinate-ascent-2025-12-22.txt \
                    features.txt \
                    train-2025-12-22.txt
```

Args:
* `dmpworks`: the feature set name.
* `model-coordinate-ascent-2025-12-22`: the model name.
* `model-coordinate-ascent-2025-12-22.txt`: the path to the RankLib model file.
* `features.txt`: the path to the features.txt file.
* `train-2025-12-22.txt`: the path to the training dataset file, used to compute normalisation information that is set in the uploaded model.

Compute ranking metrics:
```bash
dmpworks opensearch rank-metrics \
                    ground-truth-2025-12-22.csv \
                    dmps-index \
                    works-index \
                    metrics-2025-12-22.csv \
                    --query-builder-name=build_dmp_works_search_baseline_query \
                    --rerank-model-name=model-coordinate-ascent-2025-12-22 \
                    --max-results=1000 \
                    --ks=10 20 100 1000
```

Args:
* `ground-truth-2025-12-22.csv`: The path to the ground truth data file.
* `dmps-index`: The DMPs index name.
* `works-index`: The works index name.
* `metrics-2025-12-22.csv`: The path to the file where the computed metrics will be saved.

Options:
* `--query-builder-name=build_dmp_works_search_baseline_query`: the name of the baseline query to use.
* `--rerank-model-name=model-coordinate-ascent-2025-12-22`: the name of the model to use for re-ranking. Omit to test baseline search.
* `--max-results=1000`: the maximum number of works to return for each DMP.
* `--ks=10 20 100 1000`: the top K breakpoints to compute for each metric.