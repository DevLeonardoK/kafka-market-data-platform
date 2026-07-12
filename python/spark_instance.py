from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("market_data_spark")
    .master("spark://apache-spark-master:7077")
    .getOrCreate()
)

spark.stop()