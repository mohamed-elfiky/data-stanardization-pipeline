#!/bin/bash

set -e

cd "$(dirname "$0")"

BUCKET_NAME="us-central1-village-environ-a7017415-bucket"
DAGS_PATH="standardization-dags"
DESTINATION="gs://${BUCKET_NAME}/dags/"

echo "Syncing DAGs to Composer bucket: ${DESTINATION}"

gsutil -m cp -r "${DAGS_PATH}/"* "${DESTINATION}"

echo "Upload complete ✅"
