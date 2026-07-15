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
#+- ~StreamingDataSourceV2ScanRelation[key#7, value#8, topic#9, partition#10, offset#11L, timestamp#12, timestampType#13] KafkaTable
    # query = (
    #     df.select(
    #         F.col("key").cast(StringType()),
    #         F.col("value").cast(StringType()),
    #         F.col("topic"),
    #         F.col("partition"),
    #         F.col("offset"),
    #         F.col("timestamp"),
    #         F.col("timestampType").cast(StringType()),
    #     ).writeStream.format("console").option("truncate", "false")
    #     .start()
    # )


    query = (df.writeStream.format("parquet")\
    .option("path", "gs://bucket_gs_market_data_raw_bronze/raw")\
    .option("checkpointLocation", "gs://bucket_gs_market_data_raw_bronze/checkpoints")\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .start())

    query.awaitTermination()

except Exception:
    raise