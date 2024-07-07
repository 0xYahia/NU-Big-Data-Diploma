from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.streaming import DataStreamReader

# Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("KafkaSparkStreaming") \
#     .getOrCreate()
spark = SparkSession.builder \
    .appName("CarRatesStreaming") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')\
    .config("spark.jars", "../jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Configure Kafka source
kafka_brokers = ["localhost:9092", "kafka:29092"]
kafka_topics = ["Acura_stream", "Audi_stream", "BMW_stream", "Buick_stream", "Cadillac_stream", "Chevrolet_stream", "Chrysler_stream",
                "Dodge_stream", "Ford_stream", "GMC_stream", "Honda_stream", "Hyundai_stream", "INFINITI_stream", "Jaguar_stream",
                "Jeep_stream", "Kia_stream", "Land_stream", "Lexus_stream", "Lincoln_stream", "Mazda_stream", "Mercedes-Benz_stream",
                "Maserati_stream", "Mazda_stream", "Mercury_stream", "Mini_stream", "Mitsubishi_stream", "Nissan_stream",
                "Porsche_stream", "RAM_stream", "Subaru_stream", "Tesla_stream", "Toyota_stream", "Volkswagen_stream", "Volvo_stream"]


# Set Kafka consumer configuration
kafka_config = {
    "bootstrap.servers": kafka_brokers,
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "auto.offset.reset": "earliest"  # Set to consume from earliest
}

# Define schema for incoming data
schema = StructType([
    StructField("Brand", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Price", FloatType(), True),
    StructField("Mileage", FloatType(), True)
])

# Read data from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "your_topic_name") \
#     .load()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .option("subscribe", ",".join(kafka_topics)) \
    .load()

# Convert the value column to string
value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and extract the fields
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Calculate the average price per brand
avg_price_df = json_df.groupBy("Brand").agg(avg("Price").alias("avg_price"))

# Write the result to the console
query = avg_price_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()