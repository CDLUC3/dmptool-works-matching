from sqlmesh.core.macros import macro, SQL


@macro()
def array_agg_distinct(evaluator, column_name: SQL) -> str:
    return f"COALESCE(ARRAY_AGG(DISTINCT {column_name} ORDER BY LOWER({column_name}) ASC), [])"
