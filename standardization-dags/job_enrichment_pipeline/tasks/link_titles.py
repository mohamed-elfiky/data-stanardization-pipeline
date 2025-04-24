import logging
import os
from typing import Dict, Any
from airflow.decorators import task
from airflow.models import Variable

from job_enrichment_pipeline.utils.link_titles_utils import link_pre_enriched_titles

logger = logging.getLogger("task.link_titles")


@task(
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "512Mi",
            "request_cpu": "250m",
            "limit_memory": "1Gi",
            "limit_cpu": "500m",
        }
    }
)
def link_titles(file_path: str, config: Dict[str, Any] = None, **context) -> str:
    logger.info(f"Starting pre-enrichment link step for: {file_path}")

    db_uri = Variable.get("POSTGRES_URI", default_var=None) or os.environ.get(
        "POSTGRES_URI"
    )
    if not db_uri:
        raise ValueError("POSTGRES_URI environment variable not set")

    return link_pre_enriched_titles(input_path=file_path, db_uri=db_uri)
