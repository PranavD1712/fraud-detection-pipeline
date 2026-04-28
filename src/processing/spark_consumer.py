from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("hour", IntegerType()),
    StructField("day_of_week", IntegerType()),
    StructField("is_fraud", IntegerType()),
    StructField("timestamp", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

transactions = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

featured = transactions \
    .withColumn("is_high_amount", when(col("amount") > 500, 1).otherwise(0)) \
    .withColumn("is_night_hour", when((col("hour") >= 0) & (col("hour") <= 5), 1).otherwise(0)) \
    .withColumn("amount_category",
        when(col("amount") < 50, "low")
        .when(col("amount") < 200, "medium")
        .when(col("amount") < 500, "high")
        .otherwise("very_high")
    )

query = featured.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "C:/Users/PRANAV/spark-checkpoint") \
    .start()

print("Spark Streaming started... watching Kafka topic 'transactions'")
query.awaitTermination()