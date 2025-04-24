import logging
import os
import io
import json
import polars as pl
from typing import Dict, List, Any, Optional
from google.cloud import storage
from datetime import datetime

from lib.llm_clients.gemini_client import GeminiClient
from lib.prompt_management.prompt_loader import load_prompt_with_params
from job_enrichment_pipeline.schema.job_title import JobTitleEnrichment

logger = logging.getLogger("enrichment_helper")


def match_titles_with_ids(
    enriched_data: List[Dict[str, Any]], batch_df: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    logger.info("Matching titles with IDs")

    title_to_id = {item.get("title", ""): item.get("id") for item in batch_df}
    validated_data = []

    for item in enriched_data:
        try:
            enriched = JobTitleEnrichment(**item)
            title_text = enriched.title
            if title_text in title_to_id:
                enriched.original_id = title_to_id[title_text]
            validated_data.append(enriched.dict())
        except Exception as validation_error:
            logger.warning(f"Validation failed for item: {item} — {validation_error}")

    logger.info(f"Successfully matched {len(validated_data)} titles with IDs")
    return validated_data


def process_batch_from_gcs(
    batch_path: str,
    prompt_type: str,
    prompt_version: str,
    timestamp: Optional[str] = None,
    output_prefix: str = "enriched",
    config: Optional[Dict[str, Any]] = None,
) -> str:
    logger.info(f"Processing batch: {batch_path}")

    if not timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not config:
        config = {}

    model_client = GeminiClient(config)

    try:
        bucket_name, blob_path = batch_path.replace("gs://", "").split("/", 1)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        batch_content = blob.download_as_bytes()
        buf = io.BytesIO(batch_content)
        df = pl.read_parquet(buf)
        batch_df = df.to_dicts()

        titles = [item.get("title", "") for item in batch_df]
        titles_str = "\n".join(titles)
        logger.info(f"Processing {len(titles)} titles")

        formatted_prompt = load_prompt_with_params(
            prompt_type=prompt_type,
            version=prompt_version,
            params={"job_titles": titles_str},
        )

        if not formatted_prompt:
            raise ValueError(
                f"Failed to load or format prompt {prompt_type} version {prompt_version}"
            )

        response_text = model_client.process(
            prompt=formatted_prompt, schema=list[JobTitleEnrichment]
        )
        raw_response = json.loads(response_text)

        enriched_titles = match_titles_with_ids(raw_response, batch_df)

        if not enriched_titles:
            raise ValueError("No enriched titles found in the response")

        result_df = pl.DataFrame(enriched_titles)

        base_filename = os.path.basename(blob_path).split(".")[0]
        output_path = f"{output_prefix}/{timestamp}/{base_filename}.parquet"
        output_blob = bucket.blob(output_path)

        buf = io.BytesIO()
        result_df.write_parquet(buf)
        buf.seek(0)
        output_blob.upload_from_file(buf, content_type="application/octet-stream")
        logger.info(f"Saved {len(enriched_titles)} enriched titles to parquet")

        full_gcs_path = f"gs://{bucket_name}/{output_path}"
        logger.info(f"Successfully processed batch, saved to {full_gcs_path}")

        return full_gcs_path

    except Exception as e:
        logger.error(f"Error processing batch {batch_path}: {str(e)}", exc_info=True)
        raise
