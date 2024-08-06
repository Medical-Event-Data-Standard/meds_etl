from typing import Iterable, Tuple

import polars as pl


def parse_time(time: pl.Expr, time_formats: Iterable[str]) -> pl.Expr:
    return pl.coalesce(
        [time.str.to_datetime(time_format, strict=False, time_unit="us") for time_format in time_formats]
    )


def convert_generic_value_to_specific(generic_value: pl.Expr) -> Tuple[pl.Expr, pl.Expr]:
    generic_value = generic_value.str.strip_chars()

    numeric_value = generic_value.cast(pl.Float32, strict=False)

    text_value = pl.when(numeric_value.is_null()).then(generic_value).otherwise(pl.lit(None, dtype=pl.Utf8()))

    return numeric_value, text_value
