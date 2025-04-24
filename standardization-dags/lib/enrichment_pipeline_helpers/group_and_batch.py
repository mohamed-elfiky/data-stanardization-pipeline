import polars as pl
import logging
from lib.enrichment_pipeline_helpers.gcs_utils import upload_dataframe_as_parquet


logger = logging.getLogger("grouper")


def group_and_batch(
    df: pl.DataFrame,
    group_by_cols: list[str],
    agg_cols: list[str],
    output_dir: str,
    timestamp: str,
    batch_size: int,
    prefix: str = "batch",
) -> list[str]:
    logger.info(f"Grouping by {group_by_cols}, aggregating {agg_cols}")

    grouped = df.group_by(group_by_cols, maintain_order=True).agg(agg_cols)

    output_paths = []

    for i in range(0, grouped.height, batch_size):
        batch = grouped.slice(i, batch_size)
        filename = f"{prefix}_{timestamp}_{i // batch_size:03}.parquet"
        blob_path = f"{output_dir.rstrip('/')}/{prefix}/{timestamp}/{filename}"

        upload_dataframe_as_parquet(df=batch, gcs_uri=blob_path)

        output_paths.append(blob_path)

    logger.info(f"Wrote {len(output_paths)} parquet batches to GCS")
    return output_paths
