import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg, count, sum as _sum, to_date

def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: gold_product_metrics.py <ingest_date YYYY-MM-DD>")

    ingest_date = sys.argv[1]
    bucket = "ro-lakehouse-ro-lakehouse-project-dl"

    products_path = f"gs://{bucket}/silver/products/ingest_date={ingest_date}/"
    events_path   = f"gs://{bucket}/silver/events/ingest_date={ingest_date}/"
    reviews_path  = f"gs://{bucket}/silver/reviews/ingest_date={ingest_date}/"
    gold_path     = f"gs://{bucket}/gold/product_metrics/ingest_date={ingest_date}/"

    spark = SparkSession.builder.appName("gold_product_metrics").getOrCreate()

    products = spark.read.parquet(products_path)
    events   = spark.read.parquet(events_path)
    reviews  = spark.read.parquet(reviews_path)

    event_metrics = (
        events.groupBy("product_id")
        .agg(
            _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_orders"),
            _sum(when(col("event_type") == "purchase", col("order_value_usd")).otherwise(None)).alias("total_revenue"),
        )
    )

    review_metrics = (
        reviews.groupBy("product_id")
        .agg(
            avg(col("rating")).alias("avg_rating"),
            count(col("review_id")).alias("review_count"),
        )
    )

    gold = (
        products.select("product_id", "name", "brand", "category")
        .join(event_metrics, on="product_id", how="left")
        .join(review_metrics, on="product_id", how="left")
        .withColumn("ingest_date", to_date(lit(ingest_date)))  # <-- DATE type
    )

    gold.printSchema()
    print("GOLD ROWS:", gold.count())

    gold.write.mode("overwrite").parquet(gold_path)
    print("WROTE GOLD TO:", gold_path)

    spark.stop()

if __name__ == "__main__":
    main()
