from confluent_kafka import Producer
import json
import time
import random

# Producer
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Random Temoerature Data Gen
def generate_sensor_data(sensor_id):
    suhu = random.randint(60, 100)
    return {'sensor_id': sensor_id, 'suhu': suhu}

# Sensor IDs
sensor_ids = ['S1', 'S2', 'S3']

# Infinite Loop & Stream Data
try:
    while True:
        for sensor_id in sensor_ids:
            data = generate_sensor_data(sensor_id)
            producer.produce('sensor-suhu', 
                           value=json.dumps(data).encode('utf-8'))
            print(f"Mengirim data: {data['sensor_id']} - {data['suhu']}°C")
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    producer.close()
    print("Producer stopped.")