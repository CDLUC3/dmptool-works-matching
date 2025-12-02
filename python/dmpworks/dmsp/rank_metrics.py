import csv
import pathlib
from collections import defaultdict

from jsonlines import jsonlines
from ranx import evaluate, Qrels, Run

from dmpworks.model.related_work_model import RelatedWork


def load_qrels_dict(file_path: pathlib.Path) -> dict:
    """Load query relevance judgments dict"""

    qrels_dict = defaultdict(dict)

    with open(file_path, mode="r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row["status"]
            if status == "ACCEPTED":
                dmp_doi = row["dmpDoi"]
                work_doi = row["workDoi"]
                qrels_dict[dmp_doi][work_doi] = 1

    return qrels_dict


def load_run_dict(file_path: pathlib.Path, dmp_dois: set[str]) -> dict:
    """Load the Run dict which stores the relevance scores estimated by the model under evaluation"""

    run_dict = defaultdict(dict)
    with jsonlines.open(file_path) as reader:
        for row in reader:
            if row.get("dmpDoi") in dmp_dois:
                related_work = RelatedWork.model_validate(row, by_name=False, by_alias=True)
                run_dict[related_work.dmp_doi][related_work.work.doi] = row.get("score") / row.get("scoreMax")

    return run_dict


def fetch_dmp_dois(qrels_dict: dict) -> set[str]:
    return set(qrels_dict.keys())


def main(
    search_results_file: pathlib.Path,
    ground_truth_file: pathlib.Path,
):
    print("Loading data")
    qrels_dict = load_qrels_dict(ground_truth_file)
    dmp_dois = fetch_dmp_dois(qrels_dict)
    run_dict = load_run_dict(search_results_file, dmp_dois)
    print("Building Qrel object")
    qrels = Qrels.from_dict(qrels_dict)
    print("Building Run object")
    run = Run.from_dict(run_dict)
    print("Evaluating")
    print(evaluate(qrels, run, ["map@10", "ndcg@10"]))
    print(evaluate(qrels, run, ["map@100", "ndcg@100"]))


if __name__ == "__main__":
    main(
        pathlib.Path("/path/to/search-results-2025-12-02.jsonl"),
        pathlib.Path("/path/to/ground-truth-2025-12-02.csv"),
    )
