import pandas as pd
import random
import os
import json
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
os.makedirs(DATA_DIR, exist_ok=True)

NUM_BATCH_TRANSACTIONS = 5000  # For the CSV (Stores)
NUM_WEB_ORDERS = 2000          # For the JSON (Online)

# --- CONSTANTS ---
CITIES = ['New York', 'Houston', 'Miami', 'Seattle', 'Atlanta', 'Boston', 'Dallas', 'Chicago', 'San Francisco', 'Los Angeles']
STORE_TYPES = ['Convenience Store', 'Supermarket', 'Warehouse Club', 'Pharmacy', 'Specialty Store', 'Department Store']
PAYMENT_METHODS = ['Debit Card', 'Mobile Payment', 'Cash', 'Credit Card', 'PayPal'] # Added PayPal for Web
CUSTOMER_CATEGORIES = ['Student', 'Professional', 'Young Adult', 'Senior Citizen', 'Teenager', 'Middle-Aged', 'Retiree', 'Homemaker']
SEASONS = ['Winter', 'Fall', 'Summer', 'Spring']
PROMOTIONS = [None, 'BOGO (Buy One Get One)', 'Discount on Selected Items']
PRODUCTS_LIST = [
    'Sponges', 'Cereal Bars', 'Banana', 'Potatoes', 'Onions', 'Hair Gel', 'Syrup', 'Milk', 'Carrots', 
    'Canned Soup', 'Shaving Cream', 'Pickles', 'Honey', 'Mop', 'Tuna', 'BBQ Sauce', 'Yogurt', 
    'Trash Cans', 'Bread', 'Toothpaste', 'Soap', 'Ketchup', 'Butter', 'Vinegar', 'Shower Gel', 
    'Orange', 'Peanut Butter', 'Hand Sanitizer', 'Ice Cream', 'Trash Bags', 'Tissues', 'Jam', 
    'Eggs', 'Chicken', 'Rice', 'Pasta', 'Soda', 'Water', 'Coffee', 'Tea', 'Sugar', 'Salt', 'Oil', 'Flour', 'Cheese', 'Beef'
]

def generate_data():
    print(f"🚀 Generating Data Silos...")
    
    # --- 1. GENERATE STORE DATA (CSV) ---
    print(f"   Generating Store Data (CSV)...")
    data = []
    start_date = datetime(2023, 1, 1)
    start_id = 1000030001 

    for i in range(NUM_BATCH_TRANSACTIONS):
        txn_date = start_date + timedelta(days=random.randint(0, 1100))
        num_items = random.randint(1, 10)
        selected_products = random.choices(PRODUCTS_LIST, k=num_items)
        
        # Season Logic
        month = txn_date.month
        if month in [12, 1, 2]: season = 'Winter'
        elif month in [3, 4, 5]: season = 'Spring'
        elif month in [6, 7, 8]: season = 'Summer'
        else: season = 'Fall'

        row = {
            'Transaction_ID': start_id + i,
            'Date': txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            'Customer_Name': fake.name(),
            'Product': str(selected_products),
            'Total_Items': num_items,
            'Total_Cost': round(random.uniform(5.00, 150.00), 2),
            'Payment_Method': random.choice(PAYMENT_METHODS),
            'City': random.choice(CITIES),
            'Store_Type': random.choice(STORE_TYPES),
            'Discount_Applied': random.choice([True, False]),
            'Customer_Category': random.choice(CUSTOMER_CATEGORIES),
            'Season': season,
            'Promotion': random.choice(PROMOTIONS)
        }
        data.append(row)

    df = pd.DataFrame(data)
    csv_path = os.path.join(DATA_DIR, 'Retail_Transactions_Dataset_Generated.csv')
    df.to_csv(csv_path, index=False)
    print(f"   ✅ Created CSV: {csv_path}")

    # --- 2. GENERATE WEB ORDERS (JSON) - The "Online Dataset" ---
    print(f"   Generating Web Orders (JSON)...")
    web_orders = []
    
    for i in range(NUM_WEB_ORDERS):
        txn_date = start_date + timedelta(days=random.randint(0, 1100))
        num_items = random.randint(1, 5)
        # Web orders usually have nested items
        items = random.choices(PRODUCTS_LIST, k=num_items)
        
        web_orders.append({
            "order_id": f"WEB-{start_id + i}",
            "timestamp": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "customer": {
                "name": fake.name(),
                "city": random.choice(CITIES),
                "category": random.choice(CUSTOMER_CATEGORIES)
            },
            "items": items, # Nested list
            "payment": {
                "method": "PayPal" if random.random() > 0.5 else "Credit Card",
                "total": round(random.uniform(20.00, 200.00), 2)
            }
        })

    json_path = os.path.join(DATA_DIR, 'web_orders.json')
    with open(json_path, 'w') as f:
        json.dump(web_orders, f, indent=4)
    print(f"   ✅ Created JSON: {json_path}")

if __name__ == "__main__":
    generate_data()