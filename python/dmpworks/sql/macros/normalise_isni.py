from sqlmesh.core.macros import SQL, macro


@macro()
def normalise_isni(evaluator, column_name: SQL) -> str:  # noqa: ARG001
    """Normalise an ISNI by removing spaces and converting to lowercase.

    Args:
        evaluator: The SQLMesh evaluator.
        column_name: The name of the column to normalise.

    Returns:
        str: The SQL expression to normalise the ISNI.
    """
    return f"LOWER(TRIM(REPLACE({column_name}, ' ', '')))"
