#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: ./run_daily_pipeline.sh <ingest_date YYYY-MM-DD>"
  exit 1
fi

INGEST_DATE="$1"
PROJECT_ID="ro-lakehouse-project"
DATASET="lakehouse"
BUCKET="ro-lakehouse-ro-lakehouse-project-dl"
CONTAINER="ro_spark"

echo "=============================="
echo "RO LAKEHOUSE DAILY PIPELINE"
echo "Ingest date: ${INGEST_DATE}"
echo "=============================="

cd "$(dirname "$0")"

echo ""
echo "[CHECK] Docker container running: ${CONTAINER}"
docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}" || {
  echo "ERROR: Container '${CONTAINER}' is not running."
  echo "Start it first: docker compose -f docker/docker-compose.yml up -d"
  exit 1
}

echo ""
echo "[CHECK] ADC credential exists inside container"
docker exec -it "${CONTAINER}" bash -lc 'test -f "${GOOGLE_APPLICATION_CREDENTIALS}"' || {
  echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS file not found inside container."
  echo "Expected path: ${GOOGLE_APPLICATION_CREDENTIALS}"
  exit 1
}

echo ""
echo "[CHECK] GCS connector jar exists inside container"
docker exec -it "${CONTAINER}" bash -lc 'test -f /opt/jars/gcs-connector-hadoop3.jar' || {
  echo "ERROR: GCS connector jar missing at /opt/jars/gcs-connector-hadoop3.jar"
  exit 1
}

echo ""
echo "[1/4] SILVER: products"
./spark_jobs/run_spark_gcs.sh /opt/spark_jobs/silver/silver_products.py "${INGEST_DATE}"

echo ""
echo "[2/4] SILVER: events"
./spark_jobs/run_spark_gcs.sh /opt/spark_jobs/silver/silver_events.py "${INGEST_DATE}"

echo ""
echo "[3/4] SILVER: reviews"
./spark_jobs/run_spark_gcs.sh /opt/spark_jobs/silver/silver_reviews.py "${INGEST_DATE}"

echo ""
echo "[4/4] GOLD: product_metrics"
./spark_jobs/run_spark_gcs.sh /opt/spark_jobs/gold/gold_product_metrics.py "${INGEST_DATE}"

echo ""
echo "[BQ] Load GOLD Parquet -> ${PROJECT_ID}.${DATASET}.product_metrics_p"
bq --project_id="${PROJECT_ID}" load \
  --source_format=PARQUET \
  --autodetect \
  --replace \
  "${DATASET}.product_metrics_p" \
  "gs://${BUCKET}/gold/product_metrics/ingest_date=${INGEST_DATE}/*"

echo ""
echo "[VERIFY] Row count by ingest_date"
bq query --use_legacy_sql=false \
"SELECT ingest_date, COUNT(*) AS row_count
 FROM \`${PROJECT_ID}.${DATASET}.product_metrics_p\`
 GROUP BY ingest_date
 ORDER BY ingest_date;"

echo ""
echo "✅ PIPELINE COMPLETE for ${INGEST_DATE}"
