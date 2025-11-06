import pendulum

from dmpworks.dmp.model import DMPModel, Funder, FundingItem


def test_parse_dmp():
    json_data = {
        "PK": "DMP#doi.org/10.48321/D00000",
        "SK": "METADATA",
        "dmp_id": "doi.org/10.48321/D00000",
        "created": "2020-01-01",
        "registered": "2020-01-01",
        "modified": "2020-01-01",
        "title": "Title",
        "description": "Description",
        "project_start": "2020-01-01",
        "project_end": "2025-01-01",
        "affiliation_ids": ["00f809463"],
        "affiliations": ["Institute for Advanced Study"],
        "people": ["Albert Einstein"],
        "people_ids": ["0000-0000-0000-0001"],
        "funding": [
            {
                "funder": {"name": "Institute for Advanced Study", "id": "00f809463"},
                "funding_opportunity_id": None,
                "status": "planned",
                "grant_id": None,
            }
        ],
        "repo_ids": [],
        "repos": [],
        "visibility": "public",
        "featured": None,
    }
    actual = DMPModel.model_validate(json_data, strict=True)
    assert (
        DMPModel(
            dmp_id="doi.org/10.48321/D00000",
            created=pendulum.date(2020, 1, 1),
            registered=pendulum.date(2020, 1, 1),
            modified=pendulum.date(2020, 1, 1),
            title="Title",
            description="Description",
            project_start=pendulum.date(2020, 1, 1),
            project_end=pendulum.date(2025, 1, 1),
            affiliation_ids=["00f809463"],
            affiliations=["Institute for Advanced Study"],
            people=["Albert Einstein"],
            people_ids=["0000-0000-0000-0001"],
            funding=[
                FundingItem(
                    funder=Funder(name="Institute for Advanced Study", id="00f809463"),
                    funding_opportunity_id=None,
                    status="planned",
                    grant_id=None,
                )
            ],
            repo_ids=[],
            repos=[],
            visibility="public",
            featured=None,
        )
        == actual
    )
