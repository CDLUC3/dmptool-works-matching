from sqlmesh.core.macros import macro, SQL


@macro()
def extract_doi(evaluator, column_name: SQL) -> str:
    return f"nullif(lower(trim(regexp_extract({column_name}, '10\\.[0-9.]+/[^\\s]+'))), '')"
