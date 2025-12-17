import csv
import os

import vcr

from dmpworks.funders.nsf_award_id import NSFAwardID, parse_nsf_award_id
from dmpworks.funders.parser import fetch_funded_dois
from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()


def test_parse_nsf_award_id():
    inputs = []
    expected = []
    data_path = os.path.join(FIXTURES_FOLDER, "nsf_award_ids.csv")

    # Load test data
    convert = lambda s: s.strip() or None  # Convert empty strings to None
    with open(data_path, mode="r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = row["text"]
            inputs.append(text)
            expected.append(
                NSFAwardID(
                    text=text,
                    org_id=convert(row["org_id"]),
                    award_id=convert(
                        row["award_id"],
                    ),
                )
            )

    # Check that award IDs parse
    print(f"test_parse_nsf_award_id:")
    for inp, exp in zip(inputs, expected):
        print(f"input: {inp}")
        print(f"\texpected: {exp}")
        parsed = NSFAwardID.parse(inp)
        print(f"\tparsed: {parsed}")
        assert exp.award_id == parsed.award_id


def test_nsf_award_id_e2e():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_award_id_e2e.yaml")):
        nsf = parse_nsf_award_id("2132549")
        nsf.fetch_additional_metadata()
        assert nsf.org_id == "OAC"
        nsf_dict = nsf.to_dict()
        nsf_rehydrated = NSFAwardID.from_dict(nsf_dict)
        assert nsf == nsf_rehydrated
        nsf_dois = fetch_funded_dois(nsf_rehydrated)
        assert len(nsf_dois) > 0
