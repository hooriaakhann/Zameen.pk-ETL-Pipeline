import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "zameen_property_listings")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "parquet_output")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoint")

schema = (
    StructType()
    .add("title", StringType())
    .add("price", StringType())
    .add("location", StringType())
    .add("area", StringType())
    .add("bedrooms", StringType())
    .add("bathrooms", StringType())
    .add("source", StringType())
)

spark = SparkSession.builder.appName("ZameenKafkaConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

query = (
    parsed_df.writeStream.format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(once=True)
    .start()
)

query.awaitTermination()
