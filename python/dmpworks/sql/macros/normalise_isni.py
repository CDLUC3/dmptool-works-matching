from sqlmesh.core.macros import macro, SQL


@macro()
def normalise_isni(evaluator, column_name: SQL) -> str:
    return f"LOWER(TRIM(REPLACE({column_name}, ' ', '')))"
