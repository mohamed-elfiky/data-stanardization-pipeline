import polars as pl
from io import StringIO
from typing import List, Optional, Type
import logging

logger = logging.getLogger(__name__)


def parse_llm_csv_response(
    text: str, expected_columns: Optional[List[str]] = None, sample_line_count: int = 5
) -> pl.DataFrame:
    try:
        df = pl.read_csv(
            StringIO(text.strip()),
            has_header=True,
            ignore_errors=True,
            truncate_ragged_lines=True,
            infer_schema_length=1000,
        )

        if expected_columns:
            missing = set(expected_columns) - set(df.columns)
            if missing:
                logger.error(f"Missing expected columns in LLM CSV response: {missing}")
                raise ValueError(f"Missing expected columns: {missing}")

        return df

    except Exception as e:
        logger.error(f"Failed to parse LLM CSV response: {str(e)}")

        lines = text.strip().splitlines()
        logger.warning(
            "Sample LLM response lines:\n" + "\n".join(lines[:sample_line_count])
        )

        raise
