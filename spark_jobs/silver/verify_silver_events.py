from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify_silver_events").getOrCreate()

path = "gs://ro-lakehouse-ro-lakehouse-project-dl/silver/events/ingest_date=2026-03-11/"

df = spark.read.parquet(path)

print("SILVER ROWS:", df.count())
df.printSchema()
df.show(5, truncate=False)

spark.stop()
