import pandas as pd
import json
import random
import os
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# --- CONFIGURATION (SCALED UP) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
os.makedirs(DATA_DIR, exist_ok=True)

# THE "MASSIVE" NUMBERS
NUM_PRODUCTS = 50
NUM_STORES = 20
NUM_CUSTOMERS = 1000  # More customers for CLV calculation
NUM_TRANSACTIONS = 10000  # Big Data Simulation

print(f"🚀 Generating MASSIVE Dataset ({NUM_TRANSACTIONS} rows)... This may take a moment.")

# 1. PRODUCTS
products = []
categories = ['Electronics', 'Clothing', 'Home', 'Groceries', 'Beauty']
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        'product_id': f'P{i:03d}',
        'name': f"{fake.word()} {fake.word()}",
        'category': random.choice(categories),
        'unit_price': round(random.uniform(5, 500), 2)
    })

# 2. POS DATA (The "Big" File)
pos_data = []
start_date = datetime(2023, 1, 1)

for _ in range(NUM_TRANSACTIONS):
    prod = random.choice(products)
    qty = random.randint(1, 10)
    
    # Simulate a transaction
    txn = {
        'transaction_id': fake.uuid4(),
        'store_id': f'S{random.randint(1, NUM_STORES):03d}',
        'product_id': prod['product_id'],
        'quantity': qty,
        'price_at_sale': prod['unit_price'], # Price might change, so we log it here
        'total_amount': round(prod['unit_price'] * qty, 2),
        'timestamp': (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S")
    }

    # INJECT ERRORS (For Pipeline to fix) 
    chance = random.random()
    if chance < 0.02: txn['total_amount'] = -100.00  # Negative Price
    if chance < 0.01: txn['store_id'] = None         # Missing Store
    
    pos_data.append(txn)

df_pos = pd.DataFrame(pos_data)
# Save as CSV
df_pos.to_csv(os.path.join(DATA_DIR, 'pos_sales.csv'), index=False)
print(f"✅ Generated 'pos_sales.csv' with {len(df_pos)} rows.")

# 3. WEB ORDERS (JSON) - Complex Nested Data
web_orders = []
for _ in range(500): # 500 Online Orders
    cust_id = f"C{random.randint(1, NUM_CUSTOMERS):03d}"
    items = []
    
    # Market Basket Simulation: Buying multiple items together
    for _ in range(random.randint(1, 5)):
        p = random.choice(products)
        items.append({'product_id': p['product_id'], 'price': p['unit_price'], 'qty': random.randint(1, 3)})

    web_orders.append({
        'order_id': f"WEB-{fake.uuid4()[:8]}",
        'customer': {'id': cust_id, 'name': fake.name(), 'city': fake.city()},
        'items': items,
        'order_date': (start_date + timedelta(days=random.randint(0, 365))).isoformat()
    })

with open(os.path.join(DATA_DIR, 'web_orders.json'), 'w') as f:
    json.dump(web_orders, f, indent=4)
print("✅ Generated 'web_orders.json'")

# 4. INVENTORY (Warehouse)
inventory = []
for _ in range(200):
    p = random.choice(products)
    inventory.append({
        'warehouse_id': f'WH-{random.randint(1, 5)}',
        'product_id': p['product_id'],
        'stock_level': random.randint(0, 500), # 0 means Out of Stock
        'last_updated': datetime.now().strftime("%Y-%m-%d")
    })
pd.DataFrame(inventory).to_csv(os.path.join(DATA_DIR, 'warehouse_inventory.csv'), index=False)
print("✅ Generated 'warehouse_inventory.csv'")