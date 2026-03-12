from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

spark = SparkSession.builder.appName("silver_products").getOrCreate()

bucket = "ro-lakehouse-ro-lakehouse-project-dl"
ingest_date = "2026-03-11"

bronze_path = f"gs://{bucket}/bronze/source=products/ingest_date={ingest_date}/"
silver_path = f"gs://{bucket}/silver/products/ingest_date={ingest_date}/"

# Read JSONL (one JSON object per line)
df_raw = spark.read.json(bronze_path)

# Clean + enforce types
df_silver = (
    df_raw
    .select(
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
        lit(ingest_date).alias("ingest_date")
    )
    .dropna(subset=["product_id"])
    .dropDuplicates(["product_id"])
)

print("RAW COUNT:", df_raw.count())
print("SILVER COUNT:", df_silver.count())

# Write Parquet to silver
(df_silver.write.mode("overwrite").parquet(silver_path))

print("WROTE SILVER TO:", silver_path)

spark.stop()
