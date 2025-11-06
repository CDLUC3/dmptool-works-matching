from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from dmpworks.polars_expr_plugin._internal import __version__ as __version__
from polars.plugins import register_plugin_function

if TYPE_CHECKING:
    from dmpworks.polars_expr_plugin.typing import IntoExprColumn

LIB = Path(__file__).parent


def revert_inverted_index(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="revert_inverted_index",
        is_elementwise=True,
    )


def parse_datacite_affiliations(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="parse_datacite_affiliations",
        is_elementwise=True,
    )


def parse_datacite_name_identifiers(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="parse_datacite_name_identifiers",
        is_elementwise=True,
    )


def strip_markup(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="strip_markup",
        is_elementwise=True,
    )


def parse_name(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="parse_name",
        is_elementwise=True,
    )
