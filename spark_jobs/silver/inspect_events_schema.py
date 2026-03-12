from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_events_schema").getOrCreate()

bucket = "ro-lakehouse-ro-lakehouse-project-dl"
ingest_date = "2026-03-11"

bronze_path = f"gs://{bucket}/bronze/source=events/ingest_date={ingest_date}/"

df = spark.read.json(bronze_path)

df.printSchema()

spark.stop()
