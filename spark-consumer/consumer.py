from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType, FloatType
import psycopg2
import logging

# PostgreSQL connection details
DB_HOST = "postgres"
DB_NAME = "network_db"
DB_USER = "admin"
DB_PASS = "password"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Define Kafka sources
df_ofs = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "OFS_telemetry") \
    .load()

df_wss = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "WSS_16000A_telemetry") \
    .load()

# Define OFS schema
schema_ofs = StructType([
    StructField("profile", ArrayType(
        StructType([
            StructField("port_in", IntegerType(), True),
            StructField("port_out", IntegerType(), True)
        ])
    ), True),
    StructField("timestamp", StringType(), True)
])

# Define WSS schema
schema_wss = StructType([
    StructField("profile", ArrayType(
        StructType([
            StructField("frequency", FloatType(), True),
            StructField("attenuation", FloatType(), True),
            StructField("phase", FloatType(), True),
            StructField("port_number", IntegerType(), True)
        ])
    ), True),
    StructField("timestamp", StringType(), True)
])

# Deserialize JSON
df_ofs = df_ofs.selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("data", from_json(col("json_data"), schema_ofs)) \
    .select("data.*") \
    .withColumn("connection", explode(col("profile"))) \
    .select(
        col("connection.port_in").alias("port_IN"),
        col("connection.port_out").alias("port_OUT")
    )

df_wss = df_wss.selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("data", from_json(col("json_data"), schema_wss)) \
    .select("data.*") \
    .withColumn("profile", explode(col("profile"))) \
    .select(
        col("profile.frequency").alias("frequency"),
        col("profile.attenuation").alias("attenuation"),
        col("profile.phase").alias("phase"),
        col("profile.port_number").alias("port_number")
    )

# Function to write OFS data to PostgreSQL
def write_ofs_to_postgres(df, epoch_id):
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ofs_profile (
                id BIGSERIAL PRIMARY KEY, 
                port1 INTEGER NOT NULL, 
                port2 INTEGER NOT NULL, 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        data = [(row.port_IN, row.port_OUT) for row in df.collect()]
        cursor.executemany("INSERT INTO ofs_profile (port1, port2) VALUES (%s, %s)", data)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Database error (OFS): {e}")

# Function to write WSS data to PostgreSQL
def write_wss_to_postgres(df, epoch_id):
    df = df.filter(df.port_number > 0)
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wss16000a_profile (
                id SERIAL PRIMARY KEY, 
                frequency DOUBLE PRECISION NOT NULL, 
                attenuation DOUBLE PRECISION, 
                phase DOUBLE PRECISION, 
                port_number INTEGER NOT NULL, 
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        data = [(row.frequency, row.attenuation, row.phase, row.port_number) for row in df.collect()]
        cursor.executemany("INSERT INTO wss16000a_profile (frequency, attenuation, phase, port_number) VALUES (%s, %s, %s, %s)", data)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Database error (WSS): {e}")

# Start streaming
df_ofs.writeStream.foreachBatch(write_ofs_to_postgres).start()
df_wss.writeStream.foreachBatch(write_wss_to_postgres).start().awaitTermination()

