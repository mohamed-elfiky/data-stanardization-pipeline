import logging
from airflow.decorators import task
from airflow.models import Variable
from typing import Dict, Any
import os
from job_enrichment_pipeline.utils.enrich_utils_csv import process_batch_from_gcs

logger = logging.getLogger(__name__)


@task(
    executor_config={
        "KubernetesExecutor": {
            "request_memory": "1Gi",
            "request_cpu": "500m",
            "limit_memory": "1Gi",
            "limit_cpu": "1",
        }
    },
    max_active_tis_per_dag=3,
    retries=0,
)
def enrich_job_title_batch(
    batch_paths: [str], config: Dict[str, Any] = None, **context
) -> str:
    batch_path = batch_paths[0]
    logger.info(f"Enriching job title batch: {batch_path}")

    llm_config = config.get("llm", {}) if config else {}
    default_prompt_version = llm_config.get("default_prompt_version", "v1")
    prompt_version = Variable.get(
        "JOB_TITLE_PROMPT_VERSION", default_var=None
    ) or os.environ.get("JOB_TITLE_PROMPT_VERSION", default_prompt_version)
    logger.info(f"Using job title prompt version: {prompt_version}")

    return process_batch_from_gcs(
        batch_path=batch_path,
        prompt_type="job_enrichment_pipeline.prompt",
        prompt_version=prompt_version,
        timestamp=context["execution_date"].strftime("%Y%m%dT%H%M%S"),
        output_prefix="enriched/job_titles",
        config=llm_config,
    )
