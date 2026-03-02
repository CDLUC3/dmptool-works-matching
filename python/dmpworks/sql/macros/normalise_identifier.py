from sqlmesh.core.macros import macro, SQL


@macro()
def normalise_identifier(evaluator, column_name: SQL) -> str:
    return f"NULLIF(TRIM(REGEXP_REPLACE(LOWER(CAST({column_name} AS VARCHAR)), 'https?://[^/]+/', '', 'g')), '')"
