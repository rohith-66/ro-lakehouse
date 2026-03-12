from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

path = "gs://ro-lakehouse-ro-lakehouse-project-dl/bronze/source=products/ingest_date=2026-03-11/"
df = spark.read.text(path)

print("ROWS:", df.count())
df.show(5, truncate=False)

spark.stop()
