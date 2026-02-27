from sqlmesh.core.macros import macro, SQL


@macro()
def extract_doi(evaluator, column_name: SQL) -> str:
    return f"NULLIF(LOWER(TRIM(REGEXP_EXTRACT({column_name}, '10\\.[0-9.]+/[^\\s]+'))), '')"
