import json
import time
import random
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MERCHANTS = ['Amazon', 'Walmart', 'Netflix', 'Shell', 'McDonald\'s', 'Apple', 'Uber', 'Zara']
CATEGORIES = ['electronics', 'grocery', 'entertainment', 'fuel', 'food', 'subscription', 'travel', 'clothing']

def generate_transaction():
    is_fraud = random.random() < 0.05  # 5% fraud rate

    if is_fraud:
        amount = round(random.uniform(800, 5000), 2)   # frauds = high amount
        hour = random.choice([1, 2, 3, 4])             # frauds = odd hours
    else:
        amount = round(random.uniform(5, 500), 2)
        hour = random.randint(8, 22)

    return {
        "transaction_id": fake.uuid4(),
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "amount": amount,
        "merchant": random.choice(MERCHANTS),
        "category": random.choice(CATEGORIES),
        "hour": hour,
        "day_of_week": random.randint(0, 6),
        "is_fraud": int(is_fraud),
        "timestamp": datetime.now().isoformat()
    }

print("Starting transaction producer... Press Ctrl+C to stop")
count = 0
while True:
    transaction = generate_transaction()
    producer.send('transactions', value=transaction)
    count += 1
    label = "FRAUD" if transaction['is_fraud'] else "legit"
    print(f"[{count}] Sent {label} txn | amount: ${transaction['amount']} | merchant: {transaction['merchant']}")
    time.sleep(0.5)
