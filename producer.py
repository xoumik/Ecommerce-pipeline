from confluent_kafka import Producer
from faker import Faker
import json
import time
import random
import uuid

# Initialize Faker to generate realistic fake data
fake = Faker()

# Configure the Kafka Producer to talk to our Docker container
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'ecommerce-producer'
}
producer = Producer(conf)

# The Kafka Topic we will write to
TOPIC_NAME = 'ecommerce_orders'

# A list of products to randomly choose from
PRODUCTS = ['Laptop', 'Smartphone', 'Headphones', 'Monitor', 'Keyboard', 'Mouse']

def delivery_report(err, msg):
    """Callback triggered by Kafka to tell us if the message was delivered successfully."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' at offset {msg.offset()}")

def generate_order():
    """Generates a single fake e-commerce order."""
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(10.0, 2500.0), 2),
        "timestamp": fake.iso8601()
    }

if __name__ == '__main__':
    print(f"Starting E-Commerce Data Stream to Kafka Topic: {TOPIC_NAME}...")
    
    try:
        while True:
            # Generate a fake order
            order = generate_order()
            
            # Convert the Python dictionary to a JSON string, then encode to bytes
            order_json = json.dumps(order).encode('utf-8')
            
            # Send the data to Kafka
            producer.produce(topic=TOPIC_NAME, value=order_json, callback=delivery_report)
            
            # Tell Kafka to actually send the messages in its queue
            producer.poll(0)
            
            # CONTROL FREQUENCY HERE: Sleep for 1 second between orders
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\nStopping data generation...")
    finally:
        # Ensure all remaining messages in the buffer are sent before closing
        producer.flush()
        print("Producer closed.")