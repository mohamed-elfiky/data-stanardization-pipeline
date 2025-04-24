from airflow.decorators import task
from airflow.models import Variable
from lib.enrichment_pipeline_helpers.extract_and_upload import extract_and_upload
from job_enrichment_pipeline.utils.normalize_utils import normalize_title
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger("extract.task")


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
def extract_job_titles_to_gcs(config: Dict[str, Any], **context) -> Optional[str]:
    logger.info("Starting job title extraction task...")

    pipeline_config = config.get("pipeline", {}) if config else {}
    limit = pipeline_config.get("batch_size_per_dag_run")
    timestamp = context["execution_date"].strftime("%Y%m%dT%H%M%S")

    db_uri = Variable.get("POSTGRES_URI", default_var=None) or os.environ.get(
        "POSTGRES_URI"
    )
    blob_storage_base_path = Variable.get(
        "GCS_ENRICHMENT_BASE", default_var=None
    ) or os.environ.get("GCS_ENRICHMENT_BASE")

    if not db_uri or not blob_storage_base_path:
        raise ValueError(
            "Missing required environment variables: POSTGRES_URI or GCS_ENRICHMENT_BASE"
        )

    base_query = """
        SELECT id, title
        FROM member_experience
        WHERE standardized_job_id IS NULL OR title <> previous_title
    """

    query = f"{base_query} LIMIT {limit}" if limit else base_query
    logger.info(
        f"Running extraction with query: {'LIMIT ' + str(limit) if limit else 'No LIMIT'}"
    )

    return extract_and_upload(
        db_uri=db_uri,
        query=query,
        blob_storage_base_path=blob_storage_base_path,
        timestamp=timestamp,
        prefix="titles",
        clean_column="title",
        custom_normalize=normalize_title,
    )
