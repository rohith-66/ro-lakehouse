import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: silver_reviews.py <ingest_date YYYY-MM-DD>")

    ingest_date = sys.argv[1]
    bucket = "ro-lakehouse-ro-lakehouse-project-dl"

    bronze_path = f"gs://{bucket}/bronze/source=reviews/ingest_date={ingest_date}/"
    silver_path = f"gs://{bucket}/silver/reviews/ingest_date={ingest_date}/"

    spark = SparkSession.builder.appName("silver_reviews").getOrCreate()

    df_raw = spark.read.json(bronze_path)

    # Actual bronze schema:
    # ingest_ts_utc, product_id, rating, review_id, review_text, review_ts_utc, user_id
    df_silver = (
        df_raw.select(
            col("review_id").cast("string").alias("review_id"),
            col("product_id").cast("string").alias("product_id"),
            col("user_id").cast("string").alias("user_id"),
            col("rating").cast("double").alias("rating"),
            col("review_text").cast("string").alias("review_text"),
            to_timestamp(col("review_ts_utc")).alias("review_ts_utc"),
            to_timestamp(col("ingest_ts_utc")).alias("ingest_ts_utc"),
            lit(ingest_date).alias("ingest_date"),
        )
        .dropna(subset=["review_id", "product_id"])
        .dropDuplicates(["review_id"])
    )

    print("RAW COUNT:", df_raw.count())
    print("SILVER COUNT:", df_silver.count())

    df_silver.write.mode("overwrite").parquet(silver_path)
    print("WROTE SILVER TO:", silver_path)

    spark.stop()

if __name__ == "__main__":
    main()
