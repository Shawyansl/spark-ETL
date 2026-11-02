from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OnlineStoreETL").master("local[*]").getOrCreate()
print(spark.version)
spark.stop()