from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ChecksumTest").getOrCreate()
df = spark.range(1000).repartition(10)
df.count()
