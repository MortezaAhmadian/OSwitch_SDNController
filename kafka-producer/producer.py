from kafka import KafkaProducer
import json, sys
import time
from Optical_Switch.OFS import OFS
from Optical_Switch.WSS_16000A import wss16000a

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

TOPIC_OFS = "OFS_telemetry"
TOPIC_WSS_16000A = "WSS_16000A_telemetry"
ofs = OFS()
wss = wss16000a()

def retrieve_telemetry(device):
    profile = ofs.get_existing_connections() if device == 'ofs' else wss.get_existing_connections()
    return {
        "profile": profile,
        "timestamp": time.time()
    }

while True:
    telemetry_data_ofs = retrieve_telemetry('ofs')
    telemetry_data_wss_16000a = retrieve_telemetry('wss16000a')
    producer.send(TOPIC_OFS, telemetry_data_ofs)
    producer.send(TOPIC_WSS_16000A, telemetry_data_wss_16000a)
    time.sleep(1)  # 1-second interval
