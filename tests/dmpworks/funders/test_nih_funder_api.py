import os
from unittest.mock import patch

import vcr

from dmpworks.funders.nih_funder_api import (
    _pubmed_ids_to_dois,
    nih_core_project_to_appl_ids,
    nih_fetch_award_publication_dois,
    pubmed_ids_to_dois,
)
from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()


def test_nih_core_project_to_appl_ids():
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


def test_nih_fetch_award_publication_dois():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_fetch_award_publication_dois.yaml")):
        results = nih_fetch_award_publication_dois("10808782")
        assert [
            {
                "doi": "10.3390/ph17101335",
                "pmcid": "PMC11509978",
                "pmid": 39458976,
            }
        ] == results


def test_pubmed_ids_to_dois():
    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([], "pmid")
        assert 0 == func.call_count

    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([38096378] * 200, "pmid")
        assert 1 == func.call_count

    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([38096378] * 201, "pmid")
        assert 2 == func.call_count

    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([38096378] * 300, "pmid")
        assert 2 == func.call_count

    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([38096378] * 400, "pmid")
        assert 2 == func.call_count

    with patch("dmpworks.funders.nih_funder_api._pubmed_ids_to_dois") as func:
        func.return_value = []
        pubmed_ids_to_dois([38096378] * 401, "pmid")
        assert 3 == func.call_count


def test__pubmed_ids_to_dois():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "_pubmed_ids_to_dois.yaml")):
        # PubMed IDs
        results = _pubmed_ids_to_dois([39747675, 38286823, 38096378], "pmid")
        results.sort(key=lambda x: x["pmid"])
        assert [
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
        ] == results

        # PubMed PMC IDs
        results = _pubmed_ids_to_dois([10990768, 10874502, 10908861], "pmcid")
        results.sort(key=lambda x: x["pmcid"])
        assert [
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
        ] == results
