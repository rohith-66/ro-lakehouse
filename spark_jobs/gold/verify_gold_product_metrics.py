from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify_gold_product_metrics").getOrCreate()

path = "gs://ro-lakehouse-ro-lakehouse-project-dl/gold/product_metrics/ingest_date=2026-03-11/"

df = spark.read.parquet(path)

print("GOLD ROWS:", df.count())
df.printSchema()
df.show(10, truncate=False)

spark.stop()
