import csv
import os

import pytest
import vcr

from dmpworks.funders.nsf_award_id import NSFAwardID, parse_nsf_award_id
from dmpworks.funders.parser import fetch_funded_dois
from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path() / "funders"


def load_nsf_award_id_cases():
    convert = lambda s: s.strip() or None
    data_path = os.path.join(FIXTURES_FOLDER, "nsf_award_ids.csv")
    cases = []
    with open(data_path) as f:
        for row in csv.DictReader(f):
            text = row["text"]
            expected = NSFAwardID(
                text=text,
                org_id=convert(row["org_id"]),
                award_id=convert(row["award_id"]),
            )
            cases.append((text, expected))
    return cases


NSF_CASES = load_nsf_award_id_cases()


class TestParseNSFAwardID:
    @pytest.mark.parametrize(
        ("text", "expected"),
        NSF_CASES,
        ids=[f"{text} -> {expected.identifier_string()}" for text, expected in NSF_CASES],
    )
    def test_parse(self, text, expected):
        assert NSFAwardID.parse(text).award_id == expected.award_id

    def test_e2e(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_award_id_e2e.yaml")):
            nsf = parse_nsf_award_id("2132549")
            nsf.fetch_additional_metadata()
            assert nsf.org_id == "OAC"
            nsf_dict = nsf.to_dict()
            nsf_rehydrated = NSFAwardID.from_dict(nsf_dict)
            assert nsf == nsf_rehydrated
            nsf_dois = fetch_funded_dois(nsf_rehydrated)
            assert len(nsf_dois) > 0
