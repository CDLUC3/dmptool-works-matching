import csv
import os

import vcr

from dmpworks.dmp.enrichment import fetch_funded_dois
from dmpworks.funders.nih_award_id import NIHAwardID, parse_nih_award_id
from queries.dmpworks.tests.utils import get_fixtures_path

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
    with open(data_path, mode="r") as f:
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
    print(f"test_parse_nih_award_id")
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


def test_nih_award_id_e2e():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_award_id_e2e.yaml")):
        nih = parse_nih_award_id("R01HL126896")
        nih.fetch_additional_metadata()
        nih_dict = nih.to_dict()
        nih_rehydrated = NIHAwardID.from_dict(nih_dict)
        assert nih == nih_rehydrated
        nih_dois = fetch_funded_dois(nih_rehydrated)
        assert len(nih_dois) > 0
