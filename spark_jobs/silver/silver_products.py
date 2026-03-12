import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, to_date

def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: silver_products.py <ingest_date YYYY-MM-DD>")

    ingest_date = sys.argv[1]
    bucket = "ro-lakehouse-ro-lakehouse-project-dl"

    bronze_path = f"gs://{bucket}/bronze/source=products/ingest_date={ingest_date}/"
    silver_path = f"gs://{bucket}/silver/products/ingest_date={ingest_date}/"

    spark = SparkSession.builder.appName("silver_products").getOrCreate()

    df_raw = spark.read.json(bronze_path)

    df_silver = (
        df_raw.select(
            col("product_id").cast("string").alias("product_id"),
            col("sku").cast("string").alias("sku"),
            col("name").cast("string").alias("name"),
            col("brand").cast("string").alias("brand"),
            col("category").cast("string").alias("category"),
            col("price").cast("double").alias("price"),
            col("currency").cast("string").alias("currency"),
            col("inventory").cast("int").alias("inventory"),
            col("is_active").cast("boolean").alias("is_active"),
            to_timestamp(col("last_updated_utc")).alias("last_updated_utc"),
            to_date(lit(ingest_date)).alias("ingest_date"),
        )
        .dropna(subset=["product_id"])
        .dropDuplicates(["product_id"])
    )

    print("RAW COUNT:", df_raw.count())
    print("SILVER COUNT:", df_silver.count())

    df_silver.write.mode("overwrite").parquet(silver_path)
    print("WROTE SILVER TO:", silver_path)

    spark.stop()

if __name__ == "__main__":
    main()
