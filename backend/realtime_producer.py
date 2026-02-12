import requests
import time
import random
import uuid

# Configuration
API_URL = "http://127.0.0.1:8000/ingest/order"
STORES = ["Store-001", "Store-002", "Web-Store", "App-Store"]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai"]

print(" STARTING AUTOMATED DATA STREAM...")
print(f" Sending live orders to: {API_URL}")
print("Press CTRL+C to stop.")

while True:
    try:
        # 1. Generate a random order (Simulating a customer buying something)
        payload = {
            "transaction_id": str(uuid.uuid4())[:8],
            "source": random.choice(STORES),
            "product_id": f"P{random.randint(1, 50):03d}",
            "quantity": random.randint(1, 5),
            "total_amount": round(random.uniform(20, 500), 2),
            "city": random.choice(CITIES)
        }

        # 2. Push to API (The "Automated Pathway")
        response = requests.post(API_URL, json=payload)

        if response.status_code == 200:
            print(f" Sent Order: ${payload['total_amount']} from {payload['source']}")
        else:
            print(f" Failed: {response.text}")

        # 3. Wait (Simulate time between orders)
        time.sleep(3) 

    except Exception as e:
        print(f" Connection Error: {e}")
        time.sleep(5)