#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: run_spark_gcs.sh <script_path_in_container> [script_args...]"
  exit 1
fi

SCRIPT_PATH="$1"
shift || true

docker exec -it ro_spark bash -lc "
  /opt/spark/bin/spark-submit \
    --jars /opt/jars/gcs-connector-hadoop3.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=\${GOOGLE_APPLICATION_CREDENTIALS} \
    ${SCRIPT_PATH} $*
"
