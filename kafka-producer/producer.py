from kafka import KafkaProducer
import json
import time
import random
from OFS import OFS

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

TOPIC = "network_telemetry"
ofs = OFS()

def generate_telemetry():
    return {
        "device": "OFS",
        "existing_cross_conn": ofs.get_existing_cross_connects(),
        "timestamp": time.time()
    }

while True:
    telemetry_data = generate_telemetry()
    print(f"Sending: {telemetry_data}")
    producer.send(TOPIC, telemetry_data)
    time.sleep(1)  # 1-second interval
