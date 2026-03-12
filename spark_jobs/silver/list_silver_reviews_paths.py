from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("list_silver_reviews_paths").getOrCreate()
sc = spark.sparkContext

bucket = "ro-lakehouse-ro-lakehouse-project-dl"
base = f"gs://{bucket}/silver/reviews/"

hconf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(base), hconf)
path = sc._jvm.org.apache.hadoop.fs.Path(base)

if not fs.exists(path):
    print("BASE PATH DOES NOT EXIST:", base)
else:
    statuses = fs.listStatus(path)
    print("FOUND UNDER:", base)
    for st in statuses:
        print(" -", st.getPath().toString())

spark.stop()
