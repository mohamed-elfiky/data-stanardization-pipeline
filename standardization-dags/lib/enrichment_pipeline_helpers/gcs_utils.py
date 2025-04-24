import io
from google.cloud import storage
import polars as pl


def download_parquet_as_dataframe(gcs_uri: str) -> pl.DataFrame:
    bucket_name, blob_path = gcs_uri.replace("gs://", "").split("/", 1)
    buf = io.BytesIO()
    storage.Client().bucket(bucket_name).blob(blob_path).download_to_file(buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def upload_dataframe_as_parquet(df: pl.DataFrame, gcs_uri: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    bucket_name, blob_path = gcs_uri.replace("gs://", "").split("/", 1)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")
