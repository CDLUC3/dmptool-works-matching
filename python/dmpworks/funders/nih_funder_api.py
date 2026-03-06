from __future__ import annotations

from dataclasses import dataclass
import logging

import requests

from dmpworks.transform.simdjson_transforms import extract_doi
from dmpworks.utils import retry_session, to_batches

log = logging.getLogger(__name__)

PUBMED_ID_CONVERTER_MAX_IDS = 200  # The maximum number of IDs that can be supplied to the PubMed ID converter at once


@dataclass
class NIHProjectDetails:
    """Details of an NIH project.

    Attributes:
        appl_id: The application ID.
        project_num: The project number.
    """

    appl_id: str | None = None
    project_num: str | None = None


def nih_core_project_to_appl_ids(
    core_project_num: str | None = None,
    appl_type_code: str | None = None,
    activity_code: str | None = None,
    ic_code: str | None = None,
    serial_num: str | None = None,
    support_year: str | None = None,
    full_support_year: str | None = None,
    suffix_code: str | None = None,
) -> list[NIHProjectDetails]:
    """Get the NIH Application IDs associated with an NIH Core Project Number.

    Args:
        core_project_num: The NIH Core Project Number, e.g. 5UG1HD078437-07.
        appl_type_code: The application type code.
        activity_code: The activity code.
        ic_code: The institute code.
        serial_num: The serial number.
        support_year: The support year.
        full_support_year: The full support year.
        suffix_code: The suffix code.

    Returns:
        List[NIHProjectDetails]: The list of NIH Application IDs.
    """
    criteria = {"fiscal_years": []}
    if core_project_num is not None:
        # Search with core_project_num
        criteria["project_nums"] = [core_project_num]
    else:
        # Search with project_num_split
        project_num_split = {}
        if appl_type_code is not None:
            project_num_split["appl_type_code"] = appl_type_code

        if activity_code is not None:
            project_num_split["activity_code"] = activity_code

        if ic_code is not None:
            project_num_split["ic_code"] = ic_code

        if serial_num is not None:
            project_num_split["serial_num"] = serial_num

        if support_year is not None:
            project_num_split["support_year"] = support_year

        if full_support_year is not None:
            project_num_split["full_support_year"] = full_support_year

        if suffix_code is not None:
            project_num_split["suffix_code"] = suffix_code

        criteria["project_num_split"] = project_num_split

    try:
        base_url = "https://api.reporter.nih.gov/v2/projects/search"
        data = {"criteria": criteria, "include_fields": ["ApplId", "ProjectNum"], "limit": 500}
        response = retry_session().post(base_url, json=data)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return [
            NIHProjectDetails(appl_id=result.get("appl_id"), project_num=result.get("project_num"))
            for result in results
        ]

    except requests.exceptions.RequestException:
        log.exception("nih_fetch_award_publication_dois: an error occurred while fetching data")
        raise


def nih_fetch_award_publication_dois(
    appl_id: str,
    pubmed_api_email: str | None = None,
) -> list[dict]:
    """Fetch the publications associated with an NIH award.

    Args:
        appl_id: The NIH Application ID, a 7-digit numeric identifier, .e.g 10438547.
        pubmed_api_email: An email address to use when calling the PubMed API.

    Returns:
        list[dict]: A list of publication DOIs.
    """
    base_url = "https://reporter.nih.gov/services/Projects/Publications"
    params = {
        "projectId": appl_id,
    }

    try:
        # Fetch list of NIH award publications
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        rows = data.get("results", [])

        # Get the PubMed IDs and PMC IDs
        pm_ids = []
        pmc_ids = []
        for row in rows:
            pm_id = row.get("pm_id")
            pmc_id = row.get("pmc_id")
            if pm_id is not None:
                pm_ids.append(pm_id)
            elif pmc_id is not None:
                pmc_ids.append(pm_id)

        # Add DOIs to outputs
        outputs = []
        if len(pm_ids) > 0:
            outputs.extend(pubmed_ids_to_dois(pm_ids, "pmid", email=pubmed_api_email))
        if len(pmc_ids) > 0:
            outputs.extend(pubmed_ids_to_dois(pmc_ids, "pmcid", email=pubmed_api_email))

    except requests.exceptions.RequestException:
        log.exception("nih_fetch_award_publication_dois: an error occurred while fetching data")
        raise
    else:
        return outputs


def pubmed_ids_to_dois(
    ids: list[int],
    idtype: str,
    versions: str | None = "no",
    tool: str = "dmptool-workflow",
    email: str | None = None,
) -> list[dict]:
    """Call the PubMed ID converter API to convert PubMed IDs and PMC IDs to DOIs: https://pmc.ncbi.nlm.nih.gov/tools/id-converter-api/.

    Args:
        ids: A list of PubMed IDs or PMC IDs.
        idtype: What type of IDs are supplied: "pmcid", "pmid", "mid", or "doi".
        versions: Whether to return version information.
        tool: The name of the tool.
        email: An email address to set in the PubMed API.

    Returns:
        list[dict]: A list of dictionaries containing converted IDs.
    """
    outputs = []
    for batch in to_batches(ids, 200):
        outputs.extend(_pubmed_ids_to_dois(batch, idtype, versions, tool, email))
    return outputs


def _pubmed_ids_to_dois(
    ids: list[int],
    idtype: str,
    versions: str | None = "no",
    tool="dmptool-match-workflows",
    email: str | None = None,
) -> list[dict]:
    """Internal function to convert a batch of PubMed IDs to DOIs.

    Args:
        ids: List of IDs to convert.
        idtype: Type of IDs provided.
        versions: Whether to include version information.
        tool: Name of the tool making the request.
        email: Email address for API contact.

    Returns:
        list[dict]: List of converted ID dictionaries.

    Raises:
        ValueError: If parameters are invalid.
        requests.exceptions.RequestException: If the API request fails.
    """
    # Validate parameters
    if len(ids) > PUBMED_ID_CONVERTER_MAX_IDS:
        raise ValueError("pubmed_id_converter: a maximum of 200 IDs can be supplied at once")

    if idtype not in {"pmcid", "pmid", "mid", "doi"}:
        raise ValueError(
            f"pubmed_id_converter: incorrect idtype {idtype}, should be one of 'pmcid', 'pmid', 'mid', or 'doi'"
        )

    if versions not in {None, "no"}:
        raise ValueError(f"pubmed_id_converter: versions should be None or 'no', not {versions}")

    # Construct params
    base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    params = {
        "ids": ",".join([str(id) for id in ids]),
        "format": "json",
        "idtype": idtype,
        "tool": tool,
    }
    if email is not None:
        params["email"] = email
    if versions is not None:
        params["versions"] = versions

    try:
        # Fetch data
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])

        # Format outputs
        outputs = []
        for record in records:
            pmcid = str(record.get("pmcid"))
            pmid = record.get("pmid")
            doi = extract_doi(record.get("doi"))
            outputs.append({"pmcid": pmcid, "pmid": pmid, "doi": doi})

    except requests.exceptions.RequestException:
        log.exception("pubmed_id_converter: an error occurred while fetching data")
        raise
    else:
        return outputs
