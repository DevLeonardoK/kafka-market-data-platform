from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, TimestampType
try:
    spark = SparkSession.builder.appName("spark-test").master("local[1]").getOrCreate()
    df = spark.read.parquet("/opt/spark/work-dir/parquet_file.parquet")
    df.printSchema()
    
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("symbol", StringType()),
            StructField("price", DoubleType()),
            StructField("broker", StringType()),
            StructField("quantity", DoubleType()),
            StructField("currency", StringType()),
            StructField("exchange", StringType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    df = df.withColumn("market_data", F.from_json(F.col("value").cast(StringType()), schema=schema))
    df = df.select(
        F.col("key").cast(StringType()),
        F.col("market_data.*")
    )

    df.show()

except Exception:
    raise
finally:
    spark.stop()