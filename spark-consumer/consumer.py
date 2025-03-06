from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, collect_list
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType
# from prometheus_client import start_http_server, Gauge
import json
import psycopg2


# PostgreSQL connection details
DB_HOST = "postgres"
DB_NAME = "network_db"
DB_USER = "admin"
DB_PASS = "password"

# Start Prometheus metrics server
# start_http_server(8000)
# CC_GAUGE = Gauge('cross_connection', 'Cross Connection Mapping', ['device', 'port1', 'port2'])

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkPrometheus") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Kafka Stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "network_telemetry") \
    .load()

# Deserialize JSON
df = df.selectExpr("CAST(value AS STRING) as json_data")

# Define Schema for JSON parsing
schema = StructType([
    StructField("device", StringType(), True),
    StructField("existing_cross_conn", ArrayType(
        StructType([
            StructField("port_in", IntegerType(), True),
            StructField("port_out", IntegerType(), True)
        ])
    ), True),
    StructField("timestamp", StringType(), True)
])

# Convert JSON to DataFrame
df = df.withColumn("data", from_json(col("json_data"), schema)).select("data.*")

# Explode the array to get individual cross connections
df_exploded = df.withColumn("connection", explode(col("existing_cross_conn"))).select(
    col("device"),
    col("connection.port_in").alias("port_IN"),
    col("connection.port_out").alias("port_OUT")
)

# Function to write to PostgreSQL
def write_to_postgres(df, epoch_id):
    rows = df.collect()
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()
    # Clear old data
    #cursor.execute("DELETE FROM cross_connections")

    for row in rows:
        cursor.execute("INSERT INTO cross_connections (device, port1, port2) VALUES (%s, %s, %s)",
                       (row.device, row.port_IN, row.port_OUT))
    conn.commit()
    cursor.close()
    conn.close()

df_exploded.writeStream.foreachBatch(write_to_postgres).start().awaitTermination()

# Prometheus Update Function
#def update_prometheus(df, epoch_id):
#    rows = df.collect() #.groupBy("device").agg(collect_list("port_IN").alias("portin_list"), collect_list("port_OUT").alias("portout_list")).collect()
#    for row in rows:
#        CC_GAUGE.labels(device=row["device"], port1=row["port_IN"], port2=row["port_OUT"]).set(1)
        #device = row["device"]
        #ports = list(zip(row["portin_list"], row["portout_list"]))
        # Store connections as a metric with labels
        #for port1, port2 in ports:
        #    CC_GAUGE.labels(device=device, port1=port1, port2=port2).set(1)

#df_exploded.writeStream.foreachBatch(update_prometheus).start().awaitTermination()

