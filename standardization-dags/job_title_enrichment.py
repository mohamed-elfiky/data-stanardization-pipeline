from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import timedelta
from job_enrichment_pipeline.tasks.extract import extract_job_titles_to_gcs
from job_enrichment_pipeline.tasks.group_batch import group_and_batch_titles
from job_enrichment_pipeline.tasks.enrich import enrich_job_title_batch
from job_enrichment_pipeline.tasks.load import load_to_postgres
from job_enrichment_pipeline.tasks.link_titles import link_titles
from airflow.models import Variable
import logging
import os
import json
from lib.config.config_loader import load_config


logger = logging.getLogger("job_title_enrichment")

config_path = os.path.join(
    os.path.dirname(__file__), "job_enrichment_pipeline/config/pipeline_config.toml"
)
config = load_config(config_path)

logger.info(f"Loaded configuration from {config_path}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="job_title_enrichment",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["enrichment"],
    max_active_runs=1,
) as dag:
    extract_path = extract_job_titles_to_gcs(config=config)

    link_titles_path = link_titles(file_path=extract_path, config=config)

    batch_paths = group_and_batch_titles(
        parquet_gcs_path=link_titles_path, config=config
    )

    enriched_paths = enrich_job_title_batch.partial(
        config=config,
        max_active_tis_per_dag=3,
    ).expand(batch_paths=batch_paths)

    load_task = load_to_postgres(enriched_paths=enriched_paths, config=config)

    extract_path >> link_titles_path >> batch_paths >> enriched_paths >> load_task
