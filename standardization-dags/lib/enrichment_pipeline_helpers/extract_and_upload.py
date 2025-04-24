import polars as pl
import logging
from typing import Optional, Callable
from lib.enrichment_pipeline_helpers.gcs_utils import upload_dataframe_as_parquet

logger = logging.getLogger("gcs_extractor")


def extract_and_upload(
    db_uri: str,
    query: str,
    blob_storage_base_path: str,
    timestamp: str,
    prefix: str = "title",
    clean_column: str = "title",
    min_length: int = 2,
    custom_normalize: Optional[Callable[[str], str]] = None,
) -> Optional[str]:
    try:
        logger.info(f"Starting extraction from database using query")

        df = pl.read_database_uri(query=query, uri=db_uri)
        logger.info(f"Retrieved {df.shape[0]} initial records from database")

        if df.is_empty():
            logger.warning("No records to process. Skipping upload.")
            return None

        logger.info(f"Cleaning column '{clean_column}' and applying filters")

        df = df.with_columns(
            [pl.col(clean_column).str.strip_chars().alias(clean_column)]
        ).filter(
            pl.col(clean_column).is_not_null()
            & (pl.col(clean_column).str.len_chars() >= min_length)
            & (pl.col(clean_column).str.contains(r"[a-zA-Z0-9]"))
        )

        if custom_normalize:
            logger.info(
                f"Applying custom normalization function to column '{clean_column}'"
            )

            df = df.with_columns(
                [
                    pl.col(clean_column)
                    .map_elements(custom_normalize, return_dtype=pl.String)
                    .alias(clean_column)
                ]
            )

        if df.is_empty():
            logger.warning(
                "All records discarded after cleaning and filtering. Skipping upload."
            )

            return None

        blob_storage_path = f"{blob_storage_base_path.rstrip('/')}/{prefix}/{prefix}_{timestamp}.parquet"
        logger.info(
            f"Preparing to upload {df.shape[0]} filtered records to: {blob_storage_path}"
        )

        upload_dataframe_as_parquet(df, blob_storage_path)
        logger.info(f"Successfully uploaded data to {blob_storage_path}")

        return blob_storage_path

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        raise
