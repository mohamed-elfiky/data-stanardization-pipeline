import logging
from typing import Optional, List
import polars as pl

logger = logging.getLogger("enrichment_quality")


def log_enrichment_quality(
    df: pl.DataFrame,
    expected_columns: List[str],
    original_keys: Optional[set[str]] = None,
    key_column: Optional[str] = None,
    sample_limit: int = 5,
):
    total = df.height

    def safe_count_missing(col_name: str) -> int:
        if col_name in df.columns:
            return df.filter(
                pl.col(col_name).is_null()
                | (
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().str.len_chars()
                    == 0
                )
            ).height
        return total

    # Log missing values per expected column
    for col in expected_columns:
        missing = safe_count_missing(col)
        logger.info(f"Missing values in '{col}': {missing} / {total}")

    # Log dropped rows if we have a reference key list
    if original_keys and key_column and key_column in df.columns:
        present_keys = set(df[key_column].to_list())
        dropped = original_keys - present_keys
        if dropped:
            logger.warning(f"{len(dropped)} entries missing from output")
            logger.warning(f"Sample dropped: {list(dropped)[:sample_limit]}")

    # Show samples of bad rows if any expected columns are null/malformed
    if expected_columns:
        filters = [
            (
                pl.col(col).is_null()
                | (pl.col(col).cast(pl.Utf8).str.strip_chars().str.len_chars() == 0)
            )
            for col in expected_columns
            if col in df.columns
        ]
        if filters:
            bad_rows = df.filter(pl.any_horizontal(filters))
            if bad_rows.height > 0:
                logger.debug(
                    f"Sample bad rows:\n{bad_rows.head(sample_limit).to_pandas().to_string(index=False)}"
                )
