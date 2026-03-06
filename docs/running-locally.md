# Running Locally with Dataset Subset
The system can be run locally with a dataset subset composed of a subset of 
OpenAlex, DataCite and Crossref Metadata.

## 1. Runtime Configuration
This section walks through how to configure the local runtime environment, 
including environment variables, local services, and test execution needed 
before working with data or OpenSearch.


# Path to the source data directory with raw datasets. See below for expected
# structure of source datasets.
# .
# ├── crossref_metadata
# │         └── March 2025 Public Data File from Crossref
# │             ├── 0.jsonl.gz
# │             ├── 10000.jsonl.gz
# │             ├── 10001.jsonl.gz
# │             ...
# ├── datacite
# │         └── DataCite_Public_Data_File_2024
# │                   ├── dois
# │                   │         ├── updated_2011-03
# │                   │         │         ├── 2011-03.csv.gz
# │                   │         │         └── part_0000.jsonl.gz
# │                   │         ├── updated_2011-04
# │                   │         │         ├── 2011-04.csv.gz
# │                   │         │         └── part_0000.jsonl.gz
# │                   │         ...
# │                   │
# │                   ├── MANIFEST
# │                   └── README
# │
# ├── openalex
# │         └── openalex-snapshot
# │             ├── browse.html
# │             ├── data
# │             │         ├── funders
# │             │         │         ├── manifest
# │             │         │         ├── updated_date=2024-02-07
# │             │         │         │         └── part_000.gz
# │             │         │         ├── updated_date=2024-03-19
# │             │         │         │         └── part_000.gz
# │             │         │         ...
# │             │         └── works
# │             │             ├── manifest
# │             │             ├── updated_date=2023-05-17
# │             │             │         └── part_000.gz
# │             │             ├── updated_date=2023-05-29
# │             │             │         └── part_000.gz
# │             │             ...
# │             ├── LICENSE.txt
# │             ├── README.txt
# │             └── RELEASE_NOTES.txt
# └── ror
#     └──  v1.63-2025-04-03-ror-data_schema_v2.json.gz
#


### 1.1. Environment Variables
Create a `.env.local` file based on the `.env.local.example` file.

Source environment variables:
```bash
set -a && source .env.local && set +a
```

### 1.2. Local OpenSearch Stack
Run OpenSearch locally:
```bash
docker compose up
```

To view OpenSearch Dashboards go to:
http://localhost:5601

### 1.3. Help
To view detailed descriptions for a `dmpworks` command you may append `--help` 
to the command, for example:
```bash
dmpworks opensearch create-index --help
Usage: dmpworks opensearch create-index [ARGS] [OPTIONS]

Create an OpenSearch index.

╭─ Parameters ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  INDEX-NAME --index-name                        Name of the OpenSearch index to create (e.g., works). [required]                                                                                                                            │
│ *  MAPPING-FILENAME --mapping-filename            Name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json). [required]                                                                  │
│    CLIENT-CONFIG.MODE --client-config.mode        OpenSearch connection mode. local uses an unauthenticated local client; aws uses AWS SigV4-signed requests. [choices: local, aws] [default: local]                                          │
│    CLIENT-CONFIG.HOST --client-config.host        OpenSearch hostname or IP address. [default: localhost]                                                                                                                                     │
│    CLIENT-CONFIG.PORT --client-config.port        OpenSearch HTTP port. [default: 9200]                                                                                                                                                       │
│    CLIENT-CONFIG.REGION --client-config.region    AWS region (required when mode=aws).                                                                                                                                                        │
│    CLIENT-CONFIG.SERVICE --client-config.service  AWS service name for SigV4 signing (usually es).                                                                                                                                            │
│    LOG-LEVEL --log-level                          Python log level [choices: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET] [default: INFO]                                                                                                   │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

Run SQLMesh unit tests:
```bash
dmpworks sqlmesh test
```

## 2. Dataset Subset Preparation
This section describes how to generate a subset of the source datasets used for 
development and testing.

### 2.1 Download Datasets
Download datasets into a folder on your computer, including:
* Crossref Metadata: https://www.crossref.org/learning/public-data-file/
* Data Citation Corpus: https://zenodo.org/records/16901115
* DataCite: https://datafiles.datacite.org/
* OpenAlex: https://docs.openalex.org/download
* ROR: https://zenodo.org/records/18761279

You will then need to untar Crossref Metadata, unzip Data Citation Corpus, 
untar DataCite (if it is the yearly data file), and unzip ROR.

Create a folder somewhere to store these datasets, then inside it, create the 
following subfolders and copy specific portions of the datasets to them:
* `crossref_metadata`: jsonl.gz files should be directly in here.
* `data_citation_corpus`: json files should be directly in here.
* `datacite/dois`: the dois folder should be on this path.
* `openalex/openalex-snapshot/data/works`: works folder from snapshot should be on this path.
* `ror`: gzip the extracted json file, e.g. with `gzip v2.3-2026-02-24-ror-data.json`, this matches how the AWS batch process prepares this file.

### 2.2. Create Dataset Subsets
Run the following bash script to create a subset of Crossref Metadata, DataCite 
and OpenAlex Works for local use:
```bash
./bin/dataset-subsets.sh
```

## 3. Dataset Normalisation & Transformation
This section covers how to convert raw source datasets into normalised Parquet 
files and produces a unified works index using SQLMesh.

### 3.1. Normalise Source Datasets
Run the following bash script to normalise and transform source datasets into 
Parquet files:
```bash
./bin/transform-datasets.sh
```

### 3.2. Transform into Parquet Works Index
SQLMesh is used to transform the input Parquet files into a unified works
index that is then loaded into OpenSearch.

Prepare required folders: 
```bash
mkdir -p "${DATA_DIR}"/{duckdb,sqlmesh_export,matches}
```

Run the SQLMesh pipeline:
```bash
dmpworks sqlmesh plan
```

Optionally run the DuckDB UI to view tables:
```bash
duckdb ${SQLMESH__GATEWAYS__DUCKDB__CONNECTION__DATABASE} -ui
```

To view the DuckDB database: http://localhost:4213.

## 4. OpenSearch Index Setup
This section shows how to create and populates the OpenSearch indexes used for
DMP and works search, including enrichment steps required for downstream matching.

### 4.1. Create OpenSearch Indexes
Create the OpenSearch DMPs index:
```bash
dmpworks opensearch create-index dmps-index dmps-mapping.json
```

Create the OpenSearch works index:
```bash
dmpworks opensearch create-index works-index works-mapping.json
```

### 4.2. DMPs Index
Before creating the DMPs index and syncing DMPs with OpenSearch, make sure you 
have a connection to the MySQL database and configure the MySQL environment 
variables as explained in `.env.local.example`, then run:
```bash
dmpworks opensearch sync-dmps dmps-index
```

Enrich the DMPs index with additional data:
```bash
dmpworks opensearch enrich-dmps dmps-index
```

### 4.3. Works Index
Sync the works index export with the OpenSearch works index:
```bash
dmpworks opensearch sync-works works-index ${DATA_DIR}/sqlmesh_export
```

## 5. Baseline DMP Works Search
This section runs the baseline DMP-to-works search, producing an initial set of 
candidate matches without Learning to Rank re-scoring.

### 5.1. Run DMP Works Search
To search for works associated with DMPs:
```bash
dmpworks opensearch dmp-works-search dmps-index works-index ${DATA_DIR}/matches/matches-2025-12-22.jsonl \
         --dois-file=${DATA_DIR}/meta/dmp_dois.json \
         --institutions-file=${DATA_DIR}/meta/dmp_institutions.json
```

## 6. Learning to Rank
This section outlines how to train, upload, and evaluate a Learning to Rank 
(LTR) model in OpenSearch to improve DMP-to-works ranking quality.


########################################################################
# Env vars for docs/running-locally.md
########################################################################

#
OPENSEARCH_MATCHES=${DATA_DIR}/matches/matches-YYYY-MM-DD.jsonl

# The path to the ground truth data file
LTR_GROUND_TRUTH_FILE=${DATA_DIR}/ltr/ground-truth-YYYY-MM-DD.csv

# The the path to the file where the RankLib training dataset will be saved.
LTR_TRAIN_FILE=${DATA_DIR}/ltr/train-YYYY-MM-DD.txt

# The the path to the file where the RankLib training dataset will be saved.
LTR_FEATURES_FILE=${DATA_DIR}/ltr/features.txt

LTR_MODEL_FILE=${DATA_DIR}/ltr/model-coordinate-ascent-YYYY-MM-DD.txt
LTR_MODEL_NAME=model-coordinate-ascent-YYYY-MM-DD
LTR_METRICS_FILE=${DATA_DIR}/ltr/metrics-ca-YYYY-MM-DD.csv


### 6.1. Pre-requisites
The following steps must be completed before working with LTR.

In the OpenSearch console, initialise the LTR plugin:
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

Download `RankLib-2.18.jar` and save in `bin` from:<br>
https://sourceforge.net/projects/lemur/files/lemur/RankLib-2.18/

Create a `features.txt` file that instructs RankLib what features from the
feature set to use for training. Each line contains the 1-based index of a
feature to include when training the model.
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
14
15
16
```

### 6.2. Create Feature Set
Create the feature set in OpenSearch:
```bash
dmpworks opensearch create-featureset \
                    dmpworks
```

### 6.3. Generate Training Dataset
Generate the training dataset and save in RankLib format:
```bash
dmpworks opensearch generate-training-dataset \
                    ${LTR_GROUND_TRUTH_FILE} \
                    dmps-index \
                    works-index \
                    ${LTR_TRAIN_FILE} \
                    dmpworks \
                    --query-builder-name=build_dmp_works_search_baseline_query \
                    --max-results=1000 
```

### 6.4. Train LTR Model
```bash
java -jar ./bin/RankLib-2.18.jar \
          -train ${LTR_TRAIN_FILE} \
          -feature ${LTR_FEATURES_FILE} \
          -ranker 4 \
          -metric2t MAP@10 \
          -norm zscore \
          -reg 0.01 \
          -save ${LTR_MODEL_FILE}
```

Options:
* `-train ${LTR_TRAIN_FILE}`: the path to the training dataset file.
* `-feature ${LTR_FEATURES_FILE}`: the path to the features.txt file which contains the subset of features to train.
* `-ranker 4`: what ranking model to train, in this case, Coordinate Ascent.
* `-metric2t MAP@10`: what metric to use during training.
* `-norm zscore`: the type of normalisation to use.
* `-reg 0.01`: regularization parameter.
* `-save ${LTR_MODEL_FILE}`: the path to the file where the model will be saved.

### 6.5. Upload LTR model to OpenSearch
Upload the LTR model to OpenSearch. At this step, the mean and standard 
deviation are computed for each feature (to match Z-score model normalisation) 
and supplied as feature normalisation data in the uploaded OpenSearch Learning 
to Rank model.
```bash
dmpworks opensearch upload-ranklib-model \
                    dmpworks \
                    ${LTR_MODEL_NAME} \
                    ${LTR_MODEL_FILE} \
                    ${LTR_FEATURES_FILE} \
                    ${LTR_TRAIN_FILE}
```

### 6.6. Compute Ranking Metrics
Compute ranking metrics:
```bash
dmpworks opensearch rank-metrics \
                    ${LTR_GROUND_TRUTH_FILE} \
                    dmps-index \
                    works-index \
                    ${LTR_METRICS_FILE} \
                    --query-builder-name=build_dmp_works_search_baseline_query \
                    --rerank-model-name=${LTR_MODEL_NAME} \
                    --max-results=1000 \
                    --ks=10 20 100 1000
```

The ranking metrics are saved as a CSV file contains metrics computed for an 
aggregate of all DMPs and for each individual DMP.

Metrics are reported at one or more cutoff values `k`, which are user-specified 
at runtime (for example: `--ks=10 20 100 1000`). Each metric column is generated 
dynamically based on the chosen `k` values.

See the table below for a description of each column in the dataset.

| Column name   | Description                                                                                                                                                          |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dmp_doi`     | The DOI of the DMP. The value `all` represents an aggregate over all evaluated DMPs.                                                                                 |
| `dmp_title`   | The title of the DMP.                                                                                                                                                |
| `n_outputs`   | Number of outputs for a given DMP in the ground-truth dataset.                                                                                                       |
| `map@k`       | Mean Average Precision at cutoff `k`. Measures how well relevant outputs are ranked near the top of the results list, averaged across queries.                       |
| `ndcg@k`      | Normalized Discounted Cumulative Gain at cutoff `k`. Rewards highly placing relevant outputs earlier in the ranking, with logarithmic discounting by rank position.  |
| `precision@k` | Precision at cutoff `k`. Proportion of the top-`k` ranked outputs that are relevant.                                                                                 |
| `recall@k`    | Recall at cutoff `k`. Proportion of all relevant outputs that appear within the top-`k` ranked results.                                                              |

### 6.7. DMP Search
To re-run the DMP works search with LTR re-ranking add the `rerank-model-name` 
parameter:
```bash
dmpworks opensearch dmp-works-search dmps-index works-index ${OPENSEARCH_MATCHES} \
         --dois-file=${DATA_DIR}/meta/dmp_dois.json \
         --institutions-file=${DATA_DIR}/meta/dmp_institutions.json
         --query-builder-name=build_dmp_works_search_baseline_query \
         --rerank-model-name=${LTR_MODEL_NAME}
```