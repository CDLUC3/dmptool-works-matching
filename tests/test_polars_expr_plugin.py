import dmpworks.polars_expr_plugin as pe
import polars as pl
from dmpworks.transform.datacite import AFFILIATION_SCHEMA, NAME_IDENTIFIERS_SCHEMA
from polars.testing import assert_frame_equal

# When call print(df) print all columns without truncation
pl.Config.set_tbl_width_chars(1000)
pl.Config.set_tbl_cols(-1)
pl.Config.set_fmt_str_lengths(1000)


def test_revert_inverted_index():
    """Test that a string inverted index can be reverted"""

    abstract_inverted_index = ['{"The":[0],"prelims":[1],"comprise:":[2],"Half-Title":[3]}', None]
    df = pl.DataFrame(
        {"abstract_inverted_index": abstract_inverted_index},
        schema={"abstract_inverted_index": pl.String},
    )
    df = df.with_columns(
        abstract=pe.revert_inverted_index(pl.col("abstract_inverted_index")),
    )

    expected = pl.DataFrame(
        {
            "abstract_inverted_index": abstract_inverted_index,
            "abstract": ["The prelims comprise: Half-Title", None],
        },
        schema={"abstract_inverted_index": pl.String, "abstract": pl.String},
    )
    print(expected)
    assert_frame_equal(df, expected)


def test_parse_datacite_affiliations():
    """Test that DataCite affiliations can be parsed"""

    affiliation = [
        '{"name":"University One","affiliationIdentifier":"https://ror.org/000000000","affiliationIdentifierScheme":"ROR","schemeUri":"https://ror.org"}',
        '[{"name":"University Two","affiliationIdentifier":"https://ror.org/000000001","affiliationIdentifierScheme":"ROR","schemeUri":"https://ror.org"}]',
        None,
    ]
    df = pl.DataFrame(
        {"affiliation": affiliation},
        schema={"affiliation": pl.String},
    )
    df = df.with_columns(
        parsed_affiliation=pe.parse_datacite_affiliations(pl.col("affiliation")),
    )
    print(df)

    expected = pl.DataFrame(
        {
            "affiliation": affiliation,
            "parsed_affiliation": [
                [
                    {
                        "name": "University One",
                        "affiliationIdentifier": "https://ror.org/000000000",
                        "affiliationIdentifierScheme": "ROR",
                        "schemeUri": "https://ror.org",
                    }
                ],
                [
                    {
                        "name": "University Two",
                        "affiliationIdentifier": "https://ror.org/000000001",
                        "affiliationIdentifierScheme": "ROR",
                        "schemeUri": "https://ror.org",
                    }
                ],
                [],
            ],
        },
        schema={"affiliation": pl.String, "parsed_affiliation": AFFILIATION_SCHEMA},
    )
    print(expected)
    assert_frame_equal(df, expected)


def test_parse_datacite_name_identifiers():
    """Test that DataCite name identifiers can be parsed"""

    name_identifiers = [
        '{"nameIdentifier":"https://orcid.org/0000-0000-0000-1000","nameIdentifierScheme":"ORCID","schemeUri":"https://orcid.org"}',
        '[{"nameIdentifier":"https://orcid.org/0000-0000-0000-2000","nameIdentifierScheme":"ORCID","schemeUri":"https://orcid.org"}]',
        None,
    ]
    df = pl.DataFrame(
        {"nameIdentifiers": name_identifiers},
        schema={"nameIdentifiers": pl.String},
    )
    df = df.with_columns(
        parsedNameIdentifiers=pe.parse_datacite_name_identifiers(pl.col("nameIdentifiers")),
    )
    print(df)

    expected = pl.DataFrame(
        {
            "nameIdentifiers": name_identifiers,
            "parsedNameIdentifiers": [
                [
                    {
                        "nameIdentifier": "https://orcid.org/0000-0000-0000-1000",
                        "nameIdentifierScheme": "ORCID",
                        "schemeUri": "https://orcid.org",
                    }
                ],
                [
                    {
                        "nameIdentifier": "https://orcid.org/0000-0000-0000-2000",
                        "nameIdentifierScheme": "ORCID",
                        "schemeUri": "https://orcid.org",
                    }
                ],
                [],
            ],
        },
        schema={"nameIdentifiers": pl.String, "parsedNameIdentifiers": NAME_IDENTIFIERS_SCHEMA},
    )
    print(expected)
    assert_frame_equal(df, expected)
