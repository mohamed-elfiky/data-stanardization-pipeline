from airflow.decorators import task
from airflow.models import Variable
from google.cloud import storage
import polars as pl
import io
import logging
from lib.enrichment_pipeline_helpers.group_and_batch import group_and_batch
import os
from typing import Dict, Any

logger = logging.getLogger("group_and_batch_titles")


@task(
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "1Gi",
            "request_cpu": "500m",
            "limit_memory": "2Gi",
            "limit_cpu": "1",
        }
    }
)
def group_and_batch_titles(
    parquet_gcs_path: str, config: Dict[str, Any] = None, **context
) -> list[list[str]]:
    try:
        logger.info(f"Reading parquet from GCS: {parquet_gcs_path}")
        timestamp = context["execution_date"].strftime("%Y%m%dT%H%M%S")
        bucket, blob = parquet_gcs_path.replace("gs://", "").split("/", 1)

        pipeline_config = config.get("pipeline", {}) if config else {}
        batch_size = pipeline_config.get("batch_size", 1000)
        logger.info(f"Using batch size: {batch_size}")

        buf = io.BytesIO()
        storage.Client().bucket(bucket).blob(blob).download_to_file(buf)
        buf.seek(0)

        df = pl.read_parquet(buf)
        if df.is_empty():
            logger.warning("Empty dataframe after reading parquet.")
            return []

        output_dir = Variable.get(
            "GCS_ENRICHMENT_BASE", default_var=None
        ) or os.environ.get("GCS_ENRICHMENT_BASE")

        paths = group_and_batch(
            df=df,
            group_by_cols=["title"],
            agg_cols=["id"],
            output_dir=output_dir,
            batch_size=batch_size,
            timestamp=timestamp,
            prefix="batched_titles",
        )

        return [[path] for path in paths]

    except Exception as e:
        logger.error(f"Failed to group and batch titles: {e}", exc_info=True)
        raise
