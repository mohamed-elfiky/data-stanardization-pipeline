import logging
import polars as pl
from typing import Optional
from lib.enrichment_pipeline_helpers.log_enrichment_quality import (
    log_enrichment_quality,
)

logger = logging.getLogger("enrichment_quality")


def log_title_enrichment_quality(
    df: pl.DataFrame, original_titles: Optional[set[str]] = None, sample_limit: int = 5
):
    return log_enrichment_quality(
        df,
        expected_columns=["title", "department", "function", "seniority_level"],
        original_keys=original_titles,
        key_column="title",
        sample_limit=sample_limit,
    )
