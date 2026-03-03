from sqlmesh.core.macros import SQL, macro


@macro()
def extract_doi(evaluator, column_name: SQL) -> str:  # noqa: ARG001
    """Extract a DOI from a column.

    Args:
        evaluator: The SQLMesh evaluator.
        column_name: The name of the column to extract the DOI from.

    Returns:
        str: The SQL expression to extract the DOI.
    """
    return f"NULLIF(LOWER(TRIM(REGEXP_EXTRACT({column_name}, '10\\.[0-9.]+/[^\\s]+'))), '')"
