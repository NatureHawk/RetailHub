import requests
import time
import random
import json

# --- CONFIGURATION ---
API_URL = "http://127.0.0.1:8000/ingest/order"
API_KEY = "retail-hackathon-secure-key-123"

# --- DATA GENERATION LISTS ---
PRODUCTS = ['Toothpaste', 'Shampoo', 'Laptop', 'Headphones', 'Coffee', 'T-Shirt', 'Sneakers', 'Detergent', 'Milk', 'Bread']
# In realtime_producer.py
CITIES = [
    'New York', 'Houston', 'Miami', 'Seattle', 'Atlanta', 
    'Boston', 'Dallas', 'Chicago', 'San Francisco', 'Los Angeles'
]
CUSTOMERS = ['Alice Smith', 'Bob Jones', 'Charlie Brown', 'Diana Prince', 'Evan Wright', 'Fiona White', 'George Hall', 'Hannah Lee']
SEASONS = ['Winter', 'Spring', 'Summer', 'Fall']

def generate_live_order():
    return {
        "transaction_id": str(random.randint(100000, 999999)),
        "source": "RealTime-POS",
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "total_amount": round(random.uniform(10.0, 500.0), 2),
        "city": random.choice(CITIES),
        "customer_name": random.choice(CUSTOMERS), # <--- NEW
        "season": random.choice(SEASONS)           # <--- NEW
    }

def run_producer():
    print(f"🚀 Starting Real-Time Data Producer...")
    print(f"📡 Target API: {API_URL}")
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": API_KEY
    }

    while True:
        order = generate_live_order()
        try:
            response = requests.post(API_URL, json=order, headers=headers)
            if response.status_code == 200:
                print(f"✅ Sent Order: {order['product_id']} | {order['season']} | ${order['total_amount']}")
            else:
                print(f"⚠️ Error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"❌ Connection Failed: {e}")
            print("   (Is the API server running?)")
        
        # Wait 3 seconds before next order
        time.sleep(15)

if __name__ == "__main__":
    run_producer()