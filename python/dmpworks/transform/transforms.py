import polars as pl
from polars import Date


def remove_markup(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.is_not_null())
        .then(
            expr.cast(pl.String)
            .str.replace_all("&lt;", "<")
            .str.replace_all("&gt", ">")
            .str.replace_all(r"<[^>]*>", "")
            .str.replace_all(r"[\'\"]", "")
        )
        .otherwise(None)
    )


def normalise_identifier(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.is_not_null())
        .then(expr.cast(pl.String).str.to_lowercase().str.strip_chars().str.replace_all(r"^https?://[^/]+/", ""))
        .otherwise(None)
    )


def normalise_isni(expr: pl.Expr) -> pl.Expr:
    return expr.str.replace_all(" ", "").str.strip_chars().str.to_lowercase()


def extract_orcid(expr: pl.Expr) -> pl.Expr:
    # https://support.orcid.org/hc/en-us/articles/360006897674-Structure-of-the-ORCID-Identifier
    return (
        pl.when(expr.is_not_null())
        .then(expr.str.to_lowercase().str.extract(r"\d{4}-\d{4}-\d{4}-\d{3}[\dx]", group_index=0))
        .otherwise(None)
    )


def date_parts_to_date(expr: pl.Expr) -> pl.Expr:
    year = expr.list.get(0, null_on_oob=True)
    month = expr.list.get(1, null_on_oob=True)
    day = expr.list.get(2, null_on_oob=True)

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(year.is_not_null() & month.is_not_null() & day.is_not_null())
        .then(pl.datetime(year, month, day, ambiguous="null").cast(Date))
        .when(year.is_not_null() & month.is_not_null())
        .then(pl.datetime(year, month, 1, ambiguous="null").cast(Date).dt.month_end())
        .when(year.is_not_null())
        .then(pl.datetime(year, 12, 31, ambiguous="null").cast(Date))
        .otherwise(None)
    )


def make_page(first_page: pl.Expr, last_page: pl.Expr) -> pl.Expr:
    return (
        pl.when(first_page.is_null() | last_page.is_null())
        .then(None)
        .otherwise(pl.concat_str([first_page, pl.lit("-"), last_page]))
    )


def clean_string(expr: pl.Expr) -> pl.Expr:
    cleaned = expr.str.strip_chars()
    return pl.when(cleaned == "").then(None).otherwise(cleaned)


def replace_with_null(expr: pl.Expr, values: list[str]) -> pl.Expr:
    col = expr.str.strip_chars()
    return pl.when(col.str.to_lowercase().is_in([v.lower() for v in values])).then(None).otherwise(col)
