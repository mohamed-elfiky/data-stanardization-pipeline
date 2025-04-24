import os
import io
import logging
import psycopg2
from datetime import datetime
from typing import List, Dict, Tuple, Any
import polars as pl
from psycopg2.extras import execute_values
from lib.enrichment_pipeline_helpers.gcs_utils import download_parquet_as_dataframe

logger = logging.getLogger("enrichment_loader")


def gather_valid_rows(enriched_paths: List[str]) -> List[Dict[str, Any]]:
    logger.info(f"Gathering valid rows from {len(enriched_paths)} enriched file(s)")
    rows = []

    try:
        for path in enriched_paths:
            logger.info(f"Reading file: {path}")
            df = download_parquet_as_dataframe(path).filter(
                pl.all_horizontal(
                    [
                        pl.col("title").is_not_null(),
                        pl.col("department").is_not_null(),
                        pl.col("function").is_not_null(),
                        pl.col("seniority_level").is_not_null(),
                        pl.col("id").is_not_null(),
                    ]
                )
            )

            logger.info(f"Valid rows in {path}: {df.height}")
            rows.extend(df.to_dicts())

        logger.info(f"Total valid rows gathered: {len(rows)}")
        return rows

    except Exception as e:
        logger.error(f"Failed to gather valid rows: {e}", exc_info=True)
        raise


def load_enriched_to_postgres(enriched_paths: List[str], db_uri: str) -> None:
    logger.info("Starting load of enriched data to PostgreSQL")

    job_set = set()
    mapping_list = []
    update_list = []
    stats = {"jobs": 0, "mappings": 0, "member_updates": 0}

    try:
        with psycopg2.connect(db_uri) as conn:
            with conn.cursor() as cursor:
                enriched_rows = gather_valid_rows(enriched_paths)

                for row in enriched_rows:
                    job_key = (
                        row["department"],
                        row["function"],
                        row["seniority_level"],
                    )
                    job_set.add(job_key)

                logger.info(f"Inserting {len(job_set)} unique standardized job(s)")

                execute_values(
                    cursor,
                    """
                    INSERT INTO standardized_jobs (department, function, seniority)
                    VALUES %s
                    ON CONFLICT (department, function, seniority) DO NOTHING
                    """,
                    list(job_set),
                )
                cursor.execute(
                    """
                    SELECT id, department, function, seniority
                    FROM standardized_jobs
                    WHERE (department, function, seniority) IN %s
                    """,
                    (tuple(job_set),),
                )

                job_map: Dict[Tuple[str, str, str], int] = {
                    (dept, func, senior): job_id
                    for job_id, dept, func, senior in cursor.fetchall()
                }

                stats["jobs"] = len(job_map)

                logger.info(f"Fetched {stats['jobs']} job IDs")

                for row in enriched_rows:
                    job_key = (
                        row["department"],
                        row["function"],
                        row["seniority_level"],
                    )
                    job_id = job_map.get(job_key)

                    if not job_id:
                        logger.warning(f"Missing job_id for row: {row}")
                        continue

                    mapping_list.append((row["title"], job_id))
                    update_list.append(
                        (
                            job_id,
                            datetime.utcnow(),
                            row["id"][0] if isinstance(row["id"], list) else row["id"],
                        )
                    )

                if mapping_list:
                    logger.info(f"Inserting {len(mapping_list)} job title mapping(s)")

                    execute_values(
                        cursor,
                        """
                        INSERT INTO standardized_job_mappings (job_title, standardized_job_id)
                        VALUES %s
                        ON CONFLICT (job_title) DO UPDATE
                        SET standardized_job_id = EXCLUDED.standardized_job_id
                        """,
                        mapping_list,
                    )

                    stats["mappings"] = len(mapping_list)

                if update_list:
                    logger.info(f"Updating {len(update_list)} member_experience row(s)")

                    execute_values(
                        cursor,
                        """
                        UPDATE member_experience
                        SET standardized_job_id = data.standardized_job_id,
                            last_standardized_at = data.updated_at,
                            previous_title = title
                        FROM (VALUES %s) AS data(standardized_job_id, updated_at, member_id)
                        WHERE member_experience.id = data.member_id
                        """,
                        update_list,
                    )
                    stats["member_updates"] = len(update_list)

                conn.commit()
                logger.info(f"Load completed: {stats}")

    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        raise
