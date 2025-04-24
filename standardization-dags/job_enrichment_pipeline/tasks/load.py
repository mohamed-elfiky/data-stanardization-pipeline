from airflow.decorators import task
from airflow.models import Variable
import logging
import os
from typing import List, Dict, Any

from job_enrichment_pipeline.utils.load_utils import load_enriched_to_postgres

logger = logging.getLogger("airflow.task.load_postgres")


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
def load_to_postgres(
    enriched_paths: List[str],
    config: Dict[str, Any] = None,
) -> None:
    if not enriched_paths:
        logger.warning("No enriched file paths provided to load.")
        return

    db_uri = Variable.get("POSTGRES_URI", default_var=None) or os.environ.get(
        "POSTGRES_URI"
    )
    if not db_uri:
        raise ValueError(
            "POSTGRES_URI is not set as an Airflow Variable or environment variable."
        )

    prompt_version = config.get("prompt_version") if config else None

    logger.info(f"Starting load task for {len(enriched_paths)} enriched file(s)")
    load_enriched_to_postgres(
        enriched_paths=enriched_paths,
        db_uri=db_uri,
    )
