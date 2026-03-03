from sqlmesh.core.macros import SQL, macro


@macro()
def normalise_identifier(evaluator, column_name: SQL) -> str:  # noqa: ARG001
    """Normalise an identifier by removing the protocol and domain.

    Args:
        evaluator: The SQLMesh evaluator.
        column_name: The name of the column to normalise.

    Returns:
        str: The SQL expression to normalise the identifier.
    """
    return f"NULLIF(TRIM(REGEXP_REPLACE(LOWER(CAST({column_name} AS VARCHAR)), 'https?://[^/]+/', '', 'g')), '')"
