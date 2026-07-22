from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, StructField, StructType, FloatType, IntegerType

try:
    spark = (
        SparkSession.builder
        .appName("spark_gold")
        .master("spark://apache-spark-master:7077")
        .config("spark.cores.max", "1")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("symbol", StringType()),
            StructField("price", FloatType()),
            StructField("broker", StringType()),
            StructField("quantity", FloatType()),
            StructField("currency", StringType()),
            StructField("exchange", StringType()),
            StructField("timestamp", TimestampType())
        ]
    )

    df = (
        spark.readStream
        .format("parquet")
        .schema(schema)
        .load("gs://bucket_market_data_silver/data")
    )

    def write_to_bigquery(batch_df, batch_id):
        (
            batch_df.write
            .format("bigquery")
            .option("table", "premium-student-481813-p4.data_homelab_dataset.gold_data")
            .option("credentialsFile", "/opt/spark/work-dir/projeto_data_credential.json")
            .option("parentProject", "premium-student-481813-p4")
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )

    query = (
        df.writeStream
        .foreachBatch(write_to_bigquery)
        .option("checkpointLocation", "gs://bucket_market_data_gold_temp/checkpoint_gold")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

except Exception:
    raise
finally:
    spark.stop()
