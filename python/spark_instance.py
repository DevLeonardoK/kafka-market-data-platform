from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

try:
    spark = (
        SparkSession.builder
        .appName("market_data_spark")
        .master("spark://apache-spark-master:7077")
        .getOrCreate()
    )
    
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka_broker_1:5091,kafka_broker_2:5092,kafka_broker_3:5093")
        .option("subscribe", "market_data_raw_topic")
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        df.select(
            F.col("key").cast(StringType()),
            F.col("value").cast(StringType())
        ).writeStream.format("console").option("truncate", "false")
        .start()
    )

    query.awaitTermination()

except Exception:
    raise