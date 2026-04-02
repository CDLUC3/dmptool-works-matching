import csv
import os

import pytest
import vcr

from dmpworks.funders.nih_award_id import NIHAwardID, nih_awards_generate_variants, parse_nih_award_id
from dmpworks.funders.parser import fetch_funded_dois
from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path() / "funders"


def load_nih_award_id_cases():
    convert = lambda s: s.strip() or None
    data_path = os.path.join(FIXTURES_FOLDER, "nih_award_ids.csv")
    cases = []
    with open(data_path) as f:
        for row in csv.DictReader(f):
            text = row["text"]
            expected = NIHAwardID(
                text=text,
                application_type=convert(row["application_type"]),
                activity_code=convert(row["activity_code"]),
                institute_code=convert(row["institute_code"]),
                serial_number=convert(row["serial_number"]),
                support_year=convert(row["support_year"]),
                other_suffixes=convert(row["other_suffixes"]),
            )
            cases.append((text, expected))
    return cases


NIH_CASES = load_nih_award_id_cases()


class TestParseNIHAwardID:
    @pytest.mark.parametrize(
        ("text", "expected"),
        NIH_CASES,
        ids=[f"{text} -> {expected.identifier_string()}" for text, expected in NIH_CASES],
    )
    def test_parse(self, text, expected):
        assert NIHAwardID.parse(text) == expected

    def test_generate_variants_with_activity_code_and_application_type(self):
        """Regression: must not raise RuntimeError when both are set."""
        award_id = NIHAwardID(
            text="5R01AI176039-02",
            application_type="5",
            activity_code="R01",
            institute_code="AI",
            serial_number="176039",
            support_year="02",
        )
        variants = nih_awards_generate_variants(award_id)
        assert len(variants) > 0
        assert "R01AI176039" in variants

    def test_e2e(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_award_id_e2e.yaml")):
            nih = parse_nih_award_id("R01HL126896")
            nih.fetch_additional_metadata()
            nih_dict = nih.to_dict()
            nih_rehydrated = NIHAwardID.from_dict(nih_dict)
            assert nih == nih_rehydrated
            nih_dois = fetch_funded_dois(nih_rehydrated)
            assert len(nih_dois) > 0
