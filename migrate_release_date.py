"""Migrate DynamoDB tables to rename run_date/publication_date → release_date.

Two-part process:
  Part 1 (before CloudFormation deploy): Export data from current tables
    python migrate_release_date.py --env dev --step export

  Part 2 (after CloudFormation deploy recreates tables): Import data with renamed attributes
    python migrate_release_date.py --env dev --step import
"""

import argparse
import json
from decimal import Decimal
from pathlib import Path

import boto3

TABLES = {
    "dataset-releases": {
        "renames": {"publication_date": "release_date"},
    },
    "process-works-runs": {
        "renames": {
            "run_date": "release_date",
            "publication_date_openalex_works": "release_date_openalex_works",
            "publication_date_datacite": "release_date_datacite",
            "publication_date_crossref_metadata": "release_date_crossref_metadata",
            "publication_date_ror": "release_date_ror",
            "publication_date_data_citation_corpus": "release_date_data_citation_corpus",
        },
    },
    "process-dmps-runs": {
        "renames": {"run_date": "release_date"},
    },
}


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


def table_name(env, suffix):
    return f"dmpworks-{env}-{suffix}"


def export_path(env, suffix):
    return Path(f"migration-export/{env}/{suffix}.json")


def scan_all(table):
    items = []
    response = table.scan()
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])
    return items


def rename_item(item, renames):
    return {renames.get(k, k): v for k, v in item.items()}


def step_export(env):
    """Export all table data to local JSON files."""
    dynamodb = boto3.resource("dynamodb")

    for suffix in TABLES:
        name = table_name(env, suffix)
        table = dynamodb.Table(name)
        items = scan_all(table)

        path = export_path(env, suffix)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(items, cls=DecimalEncoder, indent=2))
        print(f"  Exported {len(items)} items from {name} → {path}")

    # Also export task-runs for metadata migration
    name = table_name(env, "task-runs")
    table = dynamodb.Table(name)
    items = scan_all(table)
    path = export_path(env, "task-runs")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(items, cls=DecimalEncoder, indent=2))
    print(f"  Exported {len(items)} items from {name} → {path}")

    print(f"\n  Exports saved to migration-export/{env}/")
    print(f"  Next: deploy CloudFormation to recreate tables with new key schemas")
    print(f"  Then: python migrate_release_date.py --env {env} --step import")


def step_import(env):
    """Import exported data into the recreated tables with renamed attributes."""
    dynamodb = boto3.resource("dynamodb")

    for suffix, config in TABLES.items():
        name = table_name(env, suffix)
        path = export_path(env, suffix)

        if not path.exists():
            print(f"  SKIP {name}: no export file at {path}")
            continue

        items = json.loads(path.read_text(), parse_float=Decimal)
        renamed = [rename_item(item, config["renames"]) for item in items]

        table = dynamodb.Table(name)
        with table.batch_writer() as batch:
            for item in renamed:
                batch.put_item(Item=item)

        final_count = table.scan(Select="COUNT")["Count"]
        assert final_count == len(items), f"Count mismatch for {name}: expected {len(items)}, got {final_count}"
        print(f"  Imported {len(items)} items into {name} ✓")

    # In-place metadata rename for task-runs (table not recreated, just update metadata)
    name = table_name(env, "task-runs")
    table = dynamodb.Table(name)

    updated = 0
    items = scan_all(table)
    for item in items:
        metadata = item.get("metadata", {})
        if "publication_date" not in metadata:
            continue
        metadata["release_date"] = metadata.pop("publication_date")
        table.update_item(
            Key={"run_name": item["run_name"], "run_id": item["run_id"]},
            UpdateExpression="SET metadata = :m",
            ExpressionAttributeValues={":m": metadata},
        )
        updated += 1

    print(f"  Updated {updated} task-runs metadata maps in-place ✓")


def main():
    parser = argparse.ArgumentParser(description="Migrate DynamoDB tables: run_date/publication_date → release_date")
    parser.add_argument("--env", required=True, choices=["dev", "stg", "prd"])
    parser.add_argument("--step", required=True, choices=["export", "import"])
    args = parser.parse_args()

    print(f"\n=== Step: {args.step} | Env: {args.env} ===\n")
    {"export": step_export, "import": step_import}[args.step](args.env)
    print(f"\n=== Done ===\n")


if __name__ == "__main__":
    main()
