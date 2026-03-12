import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: silver_events.py <ingest_date YYYY-MM-DD>")

    ingest_date = sys.argv[1]
    bucket = "ro-lakehouse-ro-lakehouse-project-dl"

    bronze_path = f"gs://{bucket}/bronze/source=events/ingest_date={ingest_date}/"
    silver_path = f"gs://{bucket}/silver/events/ingest_date={ingest_date}/"

    spark = SparkSession.builder.appName("silver_events").getOrCreate()

    df_raw = spark.read.json(bronze_path)

    # Flatten nested metadata safely (schema drift tolerant: missing fields become null)
    df_silver = (
        df_raw.select(
            col("event_id").cast("string").alias("event_id"),
            to_timestamp(col("event_ts_utc")).alias("event_ts_utc"),
            col("event_type").cast("string").alias("event_type"),
            to_timestamp(col("ingest_ts_utc")).alias("ingest_ts_utc"),
            col("product_id").cast("string").alias("product_id"),
            col("user_id").cast("string").alias("user_id"),
            col("metadata.browser").cast("string").alias("browser"),
            col("metadata.campaign").cast("string").alias("campaign"),
            col("metadata.device").cast("string").alias("device"),
            col("metadata.experiment_variant").cast("string").alias("experiment_variant"),
            col("metadata.order_id").cast("string").alias("order_id"),
            col("metadata.order_value_usd").cast("double").alias("order_value_usd"),
            col("metadata.referrer").cast("string").alias("referrer"),
            lit(ingest_date).alias("ingest_date"),
        )
        .dropna(subset=["event_id"])
        .dropDuplicates(["event_id"])
    )

    print("RAW COUNT:", df_raw.count())
    print("SILVER COUNT:", df_silver.count())
    df_silver.printSchema()

    df_silver.write.mode("overwrite").parquet(silver_path)
    print("WROTE SILVER TO:", silver_path)

    spark.stop()

if __name__ == "__main__":
    main()
