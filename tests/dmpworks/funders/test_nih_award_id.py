import csv
import os

from dmpworks.funders.nih_award_id import NIHAwardID, nih_awards_generate_variants, parse_nih_award_id
from dmpworks.funders.parser import fetch_funded_dois
import vcr

from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()


def test_parse_nih_award_id():
    # # print(f"input: {inp}")
    # # print(f"\texpected: {exp}")
    # parsed = NIHAwardID.parse("ZIA AI000483")
    # print(f"\tparsed: {parsed}")
    # # self.assertEqual(exp, parsed)
    #
    # # Convert award ID to application IDs
    # appl_ids = nih_core_project_to_appl_ids(
    #     appl_type_code=parsed.application_type,
    #     activity_code=parsed.activity_code,
    #     ic_code=parsed.institute_code,
    #     serial_num=parsed.serial_number,
    #     support_year=parsed.support_year,
    #     suffix_code=parsed.other_suffixes,
    # )
    # print(f"\tappl_ids: {appl_ids}")

    inputs = []
    expected = []
    data_path = os.path.join(FIXTURES_FOLDER, "nih_award_ids.csv")

    # Load test data
    convert = lambda s: s.strip() or None  # Convert empty strings to None
    with open(data_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = row["text"]
            inputs.append(text)
            expected.append(
                NIHAwardID(
                    text=text,
                    application_type=convert(row["application_type"]),
                    activity_code=convert(row["activity_code"]),
                    institute_code=convert(row["institute_code"]),
                    serial_number=convert(row["serial_number"]),
                    support_year=convert(row["support_year"]),
                    other_suffixes=convert(row["other_suffixes"]),
                )
            )

    # Check that award IDs parse
    print("test_parse_nih_award_id")
    for inp, exp in zip(inputs, expected):
        parsed = NIHAwardID.parse(inp)
        assert exp == parsed

        # # Convert award ID to application IDs
        # appl_ids = nih_core_project_to_appl_ids(
        #     appl_type_code=parsed.application_type,
        #     activity_code=parsed.activity_code,
        #     ic_code=parsed.institute_code,
        #     serial_num=parsed.serial_number,
        #     support_year=parsed.support_year,
        #     suffix_code=parsed.other_suffixes,
        # )
        # if len(appl_ids) == 0:
        #     print(f"input: {inp}")
        #     # print(f"\texpected: {exp}")
        #     print(f"\tparsed: {parsed}")
        #     print(f"\tappl_ids: {appl_ids}")


def test_nih_awards_generate_variants_with_activity_code_and_application_type():
    """Regression test: generating variants must not raise RuntimeError when both
    activity_code and application_type are set (set mutated during iteration bug)."""
    award_id = NIHAwardID(
        text="5R01AI176039-02",
        application_type="5",
        activity_code="R01",
        institute_code="AI",
        serial_number="176039",
        support_year="02",
    )
    # Should not raise RuntimeError: Set changed size during iteration
    variants = nih_awards_generate_variants(award_id)
    assert len(variants) > 0
    assert "R01AI176039" in variants


def test_nih_award_id_e2e():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_award_id_e2e.yaml")):
        nih = parse_nih_award_id("R01HL126896")
        nih.fetch_additional_metadata()
        nih_dict = nih.to_dict()
        nih_rehydrated = NIHAwardID.from_dict(nih_dict)
        assert nih == nih_rehydrated
        nih_dois = fetch_funded_dois(nih_rehydrated)
        assert len(nih_dois) > 0
