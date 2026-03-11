# Running Locally with Dataset Subset

The system can be run locally with a subset of OpenAlex, DataCite, and Crossref Metadata.

## 1. Runtime Configuration

This section covers environment variables, local services, and test setup.

### 1.1. Environment Variables

Create a `.env.local` file from `.env.local.example` and fill in the required values.

`dmpworks` loads `.env.local` automatically when invoked (the default). You can override
this using `--env-file` or the `DMPWORKS_ENV` environment variable:

```bash
# Use an explicit file
dmpworks --env-file /path/to/.env <command>

# Or control via environment variable
DMPWORKS_ENV_FILE=/path/to/.env dmpworks <command>
```

### 1.2. DATA_DIR

`DATA_DIR` is used throughout this guide as the working directory for all datasets,
transforms, and index files. Set it in your terminal at the start of each session:

```bash
export DATA_DIR=/path/to/your/data
```

This can also be defined in `.env.local` and will be picked up automatically by the
shell scripts in `bin/`.

### 1.3. Local OpenSearch Stack

Run OpenSearch locally:

```bash
docker compose up
```

To view OpenSearch Dashboards go to:
<http://localhost:5601>

### 1.4. Help

To view detailed descriptions for a `dmpworks` command append `--help` to the command,
for example:

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

## 2. Dataset Subset Preparation

This section describes how to generate a subset of the source datasets used for
development and testing.

### 2.1 Download Datasets

Download datasets from the following sources:

* Crossref Metadata: <https://www.crossref.org/learning/public-data-file/>
* Data Citation Corpus: <https://zenodo.org/records/16901115>
* DataCite: <https://datafiles.datacite.org/>
* OpenAlex: <https://docs.openalex.org/download>
* ROR: <https://zenodo.org/records/18761279>

Unpack archives as needed (untar Crossref Metadata, unzip Data Citation Corpus, untar
DataCite if it is the yearly data file). For ROR, gzip the extracted JSON file to match
the expected format, e.g.:

```bash
gzip v2.3-2026-02-24-ror-data.json
```

Once downloaded, fill in the **Upstream Source Paths** section of `.env.local` to point to each dataset.

### 2.2. Create Dataset Subsets

`dataset-subsets.sh` creates filtered copies of Crossref Metadata, DataCite, and OpenAlex
Works based on configured institutions and DOIs, writing output to `${DATA_DIR}/sources/`.
ROR and Data Citation Corpus are copied in full.

Before running, ensure the **Dataset Subsets** section of `.env.local` is configured.

Then run:

```bash
./bin/dataset-subsets.sh
```

> **Note:** If `${DATA_DIR}/sources/` already contains symlinks from `link-upstream.sh`,
> the script will prompt before replacing them.

### 2.3. Link Upstream (Alternative to Subsetting)

To run the full pipeline against upstream datasets without creating copies, use
`link-upstream.sh`. Skip this step if you are working with the subset. This 
creates symlinks in `${DATA_DIR}/sources/` pointing to the `UPSTREAM_*` paths
— skipping the subset step entirely.

Before running, ensure the **Upstream Source Paths** section of `.env.local` is configured.

```bash
./bin/link-upstream.sh
```

This is useful when working with full dataset snapshots on a machine that already has
the upstream data available.

## 3. Dataset Normalisation & Transformation

This section covers converting raw source datasets into normalised Parquet files and
producing a unified works index using SQLMesh.

### 3.1. Normalise Source Datasets

`transform-datasets.sh` reads from `${DATA_DIR}/sources/` (populated by either
`dataset-subsets.sh` or `link-upstream.sh`) and writes normalised Parquet files to
`${DATA_DIR}/transform/`.

```bash
./bin/transform-datasets.sh
```

### 3.2. Transform into Parquet Works Index

SQLMesh transforms the input Parquet files into a unified works index for loading into
OpenSearch.

Run the SQLMesh pipeline:

```bash
dmpworks sqlmesh plan
```

Optionally run the DuckDB UI to inspect tables:

```bash
duckdb ${DATA_DIR}/duckdb/db.db  -ui
```

To view the DuckDB database: <http://localhost:4213>.

## 4. OpenSearch Index Setup

This section shows how to create and populate the OpenSearch indexes used for DMP and
works search, including enrichment steps required for downstream matching.

### 4.1. DMPs Index

Before syncing DMPs, ensure you have a connection to the MySQL database and have
configured the MySQL environment variables in `.env.local`, then run:

```bash
dmpworks opensearch sync-dmps dmps-index
```

The index is created automatically if it does not exist. To create it manually:

```bash
dmpworks opensearch create-index dmps-index dmps-mapping.json
```

Enrich the DMPs index with additional data:

```bash
dmpworks opensearch enrich-dmps dmps-index
```

### 4.2. Works Index

Sync the works index export with the OpenSearch works index:

```bash
dmpworks opensearch sync-works works-index --works-index-export=${DATA_DIR}/works_index_export --doi-state-export=${DATA_DIR}/doi_state_export --run-id=YYYY-MM-DD
```

The index is created automatically if it does not exist. To create it manually:

```bash
dmpworks opensearch create-index works-index works-mapping.json
```

## 5. Baseline DMP Works Search

This section runs the baseline DMP-to-works search, producing an initial set of
candidate matches without Learning to Rank re-scoring.

### 5.1. Run DMP Works Search

To search for works associated with DMPs:

```bash
dmpworks opensearch dmp-works-search dmps-index works-index ${DATA_DIR}/matches \
         --dois-file=${DATA_DIR}/meta/dmp_dois.json \
         --institutions-file=${DATA_DIR}/meta/dmp_institutions.json
```

## 6. Learning to Rank

This section outlines how to train, upload, and evaluate a Learning to Rank (LTR) model
in OpenSearch to improve DMP-to-works ranking quality.

Set these shell variables before running the LTR commands below (substitute dates to
match your data):

```bash
OPENSEARCH_MATCHES=${DATA_DIR}/matches/matches-YYYY-MM-DD.jsonl
LTR_GROUND_TRUTH_FILE=${DATA_DIR}/ltr/ground-truth-YYYY-MM-DD.csv
LTR_TRAIN_FILE=${DATA_DIR}/ltr/train-YYYY-MM-DD.txt
LTR_FEATURES_FILE=${DATA_DIR}/ltr/features.txt
LTR_MODEL_FILE=${DATA_DIR}/ltr/model-coordinate-ascent-YYYY-MM-DD.txt
LTR_MODEL_NAME=model-coordinate-ascent-YYYY-MM-DD
LTR_METRICS_FILE=${DATA_DIR}/ltr/metrics-ca-YYYY-MM-DD.csv
```

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
<https://sourceforge.net/projects/lemur/files/lemur/RankLib-2.18/>

Create a `features.txt` file that instructs RankLib which features to use for training.
Each line contains the 1-based index of a feature to include:

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

Upload the LTR model to OpenSearch. At this step, the mean and standard deviation are
computed for each feature (to match Z-score model normalisation) and supplied as feature
normalisation data in the uploaded OpenSearch Learning to Rank model.

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

The ranking metrics are saved as a CSV file containing metrics computed for an aggregate
of all DMPs and for each individual DMP.

Metrics are reported at one or more cutoff values `k`, which are user-specified at runtime
(for example: `--ks=10 20 100 1000`). Each metric column is generated dynamically based
on the chosen `k` values.

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

To re-run the DMP works search with LTR re-ranking add the `rerank-model-name` parameter:

```bash
dmpworks opensearch dmp-works-search dmps-index works-index ${OPENSEARCH_MATCHES} \
         --dois-file=${DATA_DIR}/meta/dmp_dois.json \
         --institutions-file=${DATA_DIR}/meta/dmp_institutions.json \
         --query-builder-name=build_dmp_works_search_baseline_query \
         --rerank-model-name=${LTR_MODEL_NAME}
```
