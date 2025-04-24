import logging
import os
import io
import polars as pl
from typing import Dict, List, Any, Optional
from google.cloud import storage
from datetime import datetime

from lib.llm_clients.gemini_client import GeminiClient
from lib.enrichment_pipeline_helpers.gcs_utils import upload_dataframe_as_parquet
from lib.prompt_management.prompt_loader import load_prompt_with_params
from lib.enrichment_pipeline_helpers.parse_llm_csv_response import (
    parse_llm_csv_response,
)
from job_enrichment_pipeline.utils.log_title_enrichment_quality import (
    log_title_enrichment_quality,
)
from job_enrichment_pipeline.utils.normalize_utils import normalize_title

logger = logging.getLogger("enrichment_helper")


def match_titles_with_ids(
    enriched_df: pl.DataFrame, batch_df: pl.DataFrame
) -> pl.DataFrame:
    logger.info(
        "Matching titles with IDs (batch is normalized, enriched needs normalization)"
    )

    enriched_df = enriched_df.with_columns(
        [pl.col("title").map_elements(normalize_title).alias("title")]
    )

    return batch_df.join(enriched_df, on="title", how="left")


def process_batch_from_gcs(
    batch_path: str,
    prompt_type: str,
    prompt_version: str,
    timestamp: Optional[str] = None,
    output_prefix: str = "enriched",
    config: Optional[Dict[str, Any]] = None,
) -> str:
    logger.info(f"Processing batch: {batch_path}")

    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    config = config or {}
    model_client = GeminiClient(config)

    try:
        bucket_name, blob_path = batch_path.replace("gs://", "").split("/", 1)
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(blob_path)

        batch_content = blob.download_as_bytes()
        df_batch = pl.read_parquet(io.BytesIO(batch_content))

        titles = df_batch.get_column("title").to_list()
        logger.info(f"Sending {len(titles)} titles to Gemini")

        prompt = load_prompt_with_params(
            prompt_type=prompt_type,
            version=prompt_version,
            params={"job_titles": "\n".join(titles)},
        )

        if not prompt:
            raise ValueError(
                f"Failed to load or format prompt {prompt_type} v{prompt_version}"
            )

        response = model_client.process(prompt=prompt)
        df_response = parse_llm_csv_response(
            response,
            expected_columns=["title", "seniority_level", "department", "function"],
        )

        log_title_enrichment_quality(df_response, original_titles=set(titles))

        df_enriched = match_titles_with_ids(df_response, df_batch)

        if df_enriched.is_empty():
            raise ValueError("No enriched titles found in the response")

        base_filename = os.path.basename(blob_path).split(".")[0]
        output_path = f"{output_prefix}/{timestamp}/{base_filename}.parquet"

        blob_path  = f"gs://{bucket_name}/{output_path}"
        upload_dataframe_as_parquet(df_enriched, blob_path)

        logger.info(f"Saved {df_enriched.height} enriched titles to: {output_path}")
        return blob_path

    except Exception as e:
        logger.error(f"Error processing batch {batch_path}: {str(e)}", exc_info=True)
        raise
