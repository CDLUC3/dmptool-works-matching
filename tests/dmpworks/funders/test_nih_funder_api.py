import os
from unittest.mock import patch

import pytest
import vcr

from dmpworks.funders.nih_funder_api import (
    pubmed_ids_to_dois_batch,
    nih_core_project_to_appl_ids,
    nih_fetch_award_publication_dois,
    pubmed_ids_to_dois,
)
from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path() / "funders"


class TestNIHFunderAPI:
    def test_core_project_to_appl_ids(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_core_project_to_appl_ids.yaml")):
            results = nih_core_project_to_appl_ids("5P41GM108569-08")
            appl_ids = {result.appl_id for result in results}
            assert {
                10438551,
                10438555,
                10438547,
                10438553,
                10438548,
                10438552,
                10438554,
            } == appl_ids

    def test_fetch_award_publication_dois(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_fetch_award_publication_dois.yaml")):
            results = nih_fetch_award_publication_dois("10808782")
            assert results == [
                {
                    "doi": "10.3390/ph17101335",
                    "pmcid": "PMC11509978",
                    "pmid": 39458976,
                }
            ]

    def testpubmed_ids_to_dois_batch_raw(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "pubmed_ids_to_dois_batch.yaml")):
            # PubMed IDs
            results = pubmed_ids_to_dois_batch([39747675, 38286823, 38096378], "pmid")
            results.sort(key=lambda x: x["pmid"])
            assert results == [
                {
                    "pmid": 38096378,
                    "pmcid": "PMC10874502",
                    "doi": "10.1021/acs.jproteome.3c00430",
                },
                {
                    "pmid": 38286823,
                    "pmcid": "PMC10990768",
                    "doi": "10.1021/jasms.3c00435",
                },
                {
                    "pmid": 39747675,
                    "pmcid": "PMC12151780",
                    "doi": "10.1038/s41596-024-01091-y",
                },
            ]

            # PubMed PMC IDs
            results = pubmed_ids_to_dois_batch([10990768, 10874502, 10908861], "pmcid")
            results.sort(key=lambda x: x["pmcid"])
            assert results == [
                {
                    "pmid": 38096378,
                    "pmcid": "PMC10874502",
                    "doi": "10.1021/acs.jproteome.3c00430",
                },
                {
                    "pmid": 38431639,
                    "pmcid": "PMC10908861",
                    "doi": "10.1038/s41467-024-46240-9",
                },
                {
                    "pmid": 38286823,
                    "pmcid": "PMC10990768",
                    "doi": "10.1021/jasms.3c00435",
                },
            ]


class TestPubmedIdsToDoisBatching:
    @pytest.mark.parametrize(
        ("count", "expected_batches"),
        [
            (0, 0),
            (200, 1),
            (201, 2),
            (300, 2),
            (400, 2),
            (401, 3),
        ],
    )
    def test_batches_in_chunks_of_200(self, count, expected_batches):
        with patch("dmpworks.funders.nih_funder_api.pubmed_ids_to_dois_batch") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * count, "pmid")
            assert func.call_count == expected_batches
