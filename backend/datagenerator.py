import pandas as pd
import json
import random
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# --- CONFIGURATION & PATHS ---
# This ensures we save to the right folder relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')

# Ensure the directory exists
os.makedirs(DATA_DIR, exist_ok=True)

NUM_PRODUCTS = 20
NUM_STORES = 5
NUM_WAREHOUSES = 2
NUM_CUSTOMERS = 50
START_DATE = datetime(2024, 1, 1)

print(f"🚀 Generating Data into: {DATA_DIR}")

# 1. GENERATE PRODUCTS
products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        'product_id': f'P{i:03d}',
        'name': fake.word().capitalize() + " " + fake.word().capitalize(),
        'category': random.choice(['Electronics', 'Clothing', 'Home', 'Toys']),
        'unit_price': round(random.uniform(10, 500), 2)
    })

# 2. POS DATA (CSV)
pos_transactions = []
for _ in range(200):
    prod = random.choice(products)
    qty = random.randint(1, 5)
    price = prod['unit_price']
    
    # Dirty Data: 5% chance of negative price
    if random.random() < 0.05:
        price = -99.00
    
    pos_transactions.append({
        'transaction_id': fake.uuid4()[:8],
        'store_id': f'S{random.randint(1, NUM_STORES):03d}',
        'product_id': prod['product_id'],
        'quantity': qty,
        'total_amount': round(price * qty, 2),
        'timestamp': (START_DATE + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
    })

df_pos = pd.DataFrame(pos_transactions)
df_pos.to_csv(os.path.join(DATA_DIR, 'pos_sales.csv'), index=False)
print("✅ Generated 'pos_sales.csv'")

# 3. WEB DATA (JSON)
web_orders = []
for _ in range(50):
    cust = fake.profile()
    basket = []
    for _ in range(random.randint(1, 3)):
        p = random.choice(products)
        basket.append({'product_id': p['product_id'], 'price': p['unit_price'], 'qty': random.randint(1, 2)})

    web_orders.append({
        'order_id': f"WEB-{random.randint(1000, 9999)}",
        'customer': {'id': f"C{random.randint(1, NUM_CUSTOMERS):03d}", 'name': cust['name'], 'address': {'city': fake.city()}},
        'items': basket,
        'order_date': (START_DATE + timedelta(days=random.randint(0, 30))).isoformat(),
        'status': random.choice(['Shipped', 'Pending'])
    })

with open(os.path.join(DATA_DIR, 'web_orders.json'), 'w') as f:
    json.dump(web_orders, f, indent=4)
print("✅ Generated 'web_orders.json'")

# 4. WAREHOUSE DATA (CSV)
inventory_records = []
for wh_id in range(1, NUM_WAREHOUSES + 1):
    for prod in products:
        inventory_records.append({
            'warehouse_id': f'WH-{wh_id:02d}',
            'product_id': prod['product_id'],
            'stock_level': random.randint(0, 100),
            'last_updated': datetime.now().strftime("%Y-%m-%d")
        })

df_inv = pd.DataFrame(inventory_records)
df_inv.to_csv(os.path.join(DATA_DIR, 'warehouse_inventory.csv'), index=False)
print("✅ Generated 'warehouse_inventory.csv'")