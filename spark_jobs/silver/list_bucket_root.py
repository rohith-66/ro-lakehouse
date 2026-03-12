from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("list_bucket_root").getOrCreate()
sc = spark.sparkContext

bucket = "ro-lakehouse-ro-lakehouse-project-dl"
base = f"gs://{bucket}/"

hconf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(base), hconf)
path = sc._jvm.org.apache.hadoop.fs.Path(base)

print("FOUND UNDER:", base)
for st in fs.listStatus(path):
    print(" -", st.getPath().toString())

spark.stop()
