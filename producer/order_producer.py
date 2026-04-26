import json
import time
import uuid
import random
import argparse
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "orders"

CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home & Garden"]

PRODUCTS = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch", "Camera", "Monitor"],
    "Clothing":    ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie", "Boots"],
    "Food":        ["Coffee Beans", "Organic Tea", "Protein Bar", "Granola", "Nuts", "Olive Oil"],
    "Books":       ["Python Cookbook", "Data Engineering", "Clean Code", "System Design", "ML Basics"],
    "Sports":      ["Running Shoes", "Yoga Mat", "Dumbbells", "Tennis Racket", "Bicycle", "Helmet"],
    "Home & Garden": ["Plant Pot", "Coffee Maker", "Pillow", "LED Light", "Toolkit", "Blender"],
}

PRICE_RANGE = {
    "Electronics":   (50.0,  1500.0),
    "Clothing":      (10.0,   200.0),
    "Food":          (3.0,     80.0),
    "Books":         (8.0,     60.0),
    "Sports":        (15.0,   500.0),
    "Home & Garden": (10.0,   300.0),
}


def generate_order() -> dict:
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    low, high = PRICE_RANGE[category]
    return {
        "order_id":  str(uuid.uuid4()),
        "user_id":   str(uuid.uuid4()),
        "product":   product,
        "category":  category,
        "amount":    round(random.uniform(low, high), 2),
        "quantity":  random.randint(1, 5),
        "status":    random.choice(["pending", "confirmed", "shipped"]),
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt}/{retries}). Retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def main(rate: float, max_orders: int):
    producer = create_producer()
    orders_sent = 0

    print(f"Producing orders to topic '{KAFKA_TOPIC}' at ~{1/rate:.1f} orders/sec. Press Ctrl+C to stop.\n")

    try:
        while max_orders == 0 or orders_sent < max_orders:
            order = generate_order()
            producer.send(KAFKA_TOPIC, key=order["order_id"], value=order)
            orders_sent += 1
            print(
                f"[{orders_sent:>5}] {order['order_id'][:8]}  "
                f"{order['category']:<15} {order['product']:<20} "
                f"${order['amount']:>8.2f}  qty:{order['quantity']}"
            )
            time.sleep(rate)
    except KeyboardInterrupt:
        print(f"\nInterrupted. Total orders sent: {orders_sent}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka order event producer")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Seconds between messages (default: 1.0)")
    parser.add_argument("--max-orders", type=int, default=0,
                        help="Stop after N orders. 0 = run forever (default: 0)")
    args = parser.parse_args()
    main(rate=args.rate, max_orders=args.max_orders)
