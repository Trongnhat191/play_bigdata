# producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Khởi tạo producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tên topic
TOPIC_NAME = "example_topic"

# Tạo dữ liệu mẫu
def generate_data():
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(40, 80), 2),
        "sensor_id": random.randint(1, 5)
    }

# Gửi dữ liệu
def send_data():
    try:
        while True:
            data = generate_data()
            producer.send(TOPIC_NAME, value=data)
            print(f"Sent data: {data}")
            time.sleep(2)  # Đợi 2 giây trước khi gửi dữ liệu tiếp
    except KeyboardInterrupt:
        producer.close()
        print("\nProducer stopped")

if __name__ == "__main__":
    send_data()