from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. Initialize Spark with BOTH Kafka and Postgres (JDBC) drivers
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the Schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

print("Spark Session created. Waiting for data from Kafka...")

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. The Magic: Write to PostgreSQL using foreachBatch
# Streaming data implies infinite data. Relational databases like Postgres prefer finite "batches".
# We use foreachBatch to take each micro-batch of streaming data and write it to Postgres.
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
        .option("dbtable", "orders") \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} saved to Postgres successfully!")

# 5. Start the stream
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()