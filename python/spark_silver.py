from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, FloatType, TimestampType, IntegerType
try:

    spark = SparkSession.builder.master("spark://apache-spark-master:7077").appName("spark_silver_batch").config("spark.cores.max","1").getOrCreate()

    schema_parquet = StructType(
        [
            StructField("key", StringType()),
            StructField("value", StringType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("timestampType", IntegerType()),
        ]
    )

    df = (
            spark.readStream.format("parquet")
            .schema(schema_parquet)
            .load("gs://bucket_market_data_bronze/raw")
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
    
    df = df.withColumn("market_data", F.from_json(F.col("value").cast(StringType()), schema=schema))
    df = df.select(
        F.col("key").cast(StringType()),
        F.col("market_data.*")
    )

    df = df.withColumn("price", F.round(F.col("price"),2))
    df = df.toDF(*[coluna.strip() for coluna in df.columns])
    string_columns = [campo.name for campo in df.schema.fields if isinstance(campo.dataType, StringType)]
    df = df.select(
        *[
            F.trim(F.col(coluna)).alias(coluna) if coluna in string_columns else F.col(coluna)
            for coluna in df.columns
        ]
    )
    query = (
        df.writeStream.format("parquet")\
        .option("path", "gs://bucket_market_data_silver/data")\
        .option("checkpointLocation", "gs://bucket_market_data_silver/checkpoint_silver") \
        .outputMode("append")\
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()

except Exception:
    raise
finally:
    spark.stop()