from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_events").getOrCreate()

bucket = "ro-lakehouse-ro-lakehouse-project-dl"
ingest_date = "2026-03-11"

bronze_path = f"gs://{bucket}/bronze/source=events/ingest_date={ingest_date}/"

df = spark.read.json(bronze_path)

print("ROW COUNT:", df.count())
df.printSchema()
df.show(5, truncate=False)

spark.stop()
