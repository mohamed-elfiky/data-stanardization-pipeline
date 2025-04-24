import io
import logging
import polars as pl
from google.cloud import storage
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger("utils.link_pre_enrich")


def link_pre_enriched_titles(input_path: str, db_uri: str) -> str:
    logger.info(f"Linking pre-enriched titles from: {input_path}")

    try:
        storage_client = storage.Client()
        bucket_name, blob_path = input_path.replace("gs://", "").split("/", 1)

        buf = io.BytesIO()
        storage_client.bucket(bucket_name).blob(blob_path).download_to_file(buf)
        buf.seek(0)
        df = pl.read_parquet(buf)

        if df.is_empty():
            logger.warning("No titles to link.")
            return input_path

        query = "SELECT job_title, standardized_job_id FROM standardized_job_mappings"
        mapping_df = pl.read_database_uri(query=query, uri=db_uri)
        logger.info(f"Fetched {len(mapping_df)} existing title mappings from DB")

        matched_df = df.join(
            mapping_df, left_on="title", right_on="job_title", how="inner"
        )
        unmatched_df = df.join(
            mapping_df, left_on="title", right_on="job_title", how="anti"
        )

        logger.info(f"Matched {matched_df.height} out of {df.height} total titles")

        update_matched_titles(matched_df, db_uri)
        return save_unmatched_titles(unmatched_df, input_path, storage_client)

    except Exception as e:
        logger.error(f"Error linking pre-enriched titles: {e}", exc_info=True)
        raise


def update_matched_titles(matched_df: pl.DataFrame, db_uri: str) -> None:
    logger.info("Updating matched titles in DB")

    if matched_df.is_empty():
        logger.info("No matched titles to update.")
        return

    update_data = [
        (row["standardized_job_id"], row["id"]) for row in matched_df.to_dicts()
    ]

    logger.info(f"Updating {len(update_data)} member_experience rows directly")
    logger.debug(f"Sample ids of updated rows: {update_data[0:10]}")

    try:
        with psycopg2.connect(db_uri) as conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    UPDATE member_experience
                    SET standardized_job_id = data.standardized_job_id
                    FROM (VALUES %s) AS data(standardized_job_id, id)
                    WHERE member_experience.id = data.id
                    """,
                    update_data,
                    template="(%s, %s)",
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error while updating matched titles: {e}", exc_info=True)
        raise


def save_unmatched_titles(
    unmatched_df: pl.DataFrame, original_path: str, storage_client: storage.Client
) -> str:
    logger.info("Saving unmatched titles for enrichment")

    try:
        if unmatched_df.is_empty():
            logger.info("No unmatched titles to save for enrichment.")
            return original_path

        output_path = original_path.replace("titles/", "titles_to_enrich/")
        output_buf = io.BytesIO()
        unmatched_df.write_parquet(output_buf)
        output_buf.seek(0)
        bucket_name, blob_path = output_path.replace("gs://", "").split("/", 1)

        storage_client.bucket(bucket_name).blob(blob_path).upload_from_file(output_buf)

        output_uri = f"gs://{bucket_name}/{blob_path}"
        logger.info(
            f"Saved {unmatched_df.height} unmatched titles for enrichment to: {output_uri}"
        )
        return output_uri
    except Exception as e:
        logger.error(f"Failed to save unmatched titles for enrichment: {e}")
        raise
