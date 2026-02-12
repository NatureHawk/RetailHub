import pandas as pd
import json
import sqlite3
import os
import ast # To parse the string lists "['A', 'B']"
import random
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

def init_star_schema(conn):
    print("🏗️ Building Star Schema Tables...")
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS Fact_Sales')
    cursor.execute('DROP TABLE IF EXISTS Fact_Inventory')
    cursor.execute('DROP TABLE IF EXISTS Fact_Shipments')
    cursor.execute('DROP TABLE IF EXISTS Dim_Product')
    cursor.execute('DROP TABLE IF EXISTS Dim_Customer')

    # 1. DIMENSION: Products
    cursor.execute('''
        CREATE TABLE Dim_Product (
            product_key TEXT PRIMARY KEY,
            name TEXT,
            category TEXT
        )
    ''')
    
    # 2. DIMENSION: Customers
    cursor.execute('''
        CREATE TABLE Dim_Customer (
            customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            category TEXT,
            city TEXT
        )
    ''')

    # 3. FACT: Sales
    cursor.execute('''
        CREATE TABLE Fact_Sales (
            transaction_id TEXT,
            date_key DATE,
            product_key TEXT,
            quantity INTEGER,
            total_amount REAL,
            city TEXT,
            payment_method TEXT,
            customer_name TEXT
        )
    ''')

    # 4. FACT: Inventory (Generated)
    cursor.execute('''
        CREATE TABLE Fact_Inventory (
            product_key TEXT,
            stock_level INTEGER,
            turnover_ratio REAL
        )
    ''')

    # 5. FACT: Shipments (Generated)
    cursor.execute('''
        CREATE TABLE Fact_Shipments (
            transaction_id TEXT,
            delivery_days INTEGER,
            status TEXT
        )
    ''')
    conn.commit()

def run_pipeline():
    conn = sqlite3.connect(DB_PATH)
    init_star_schema(conn)

    # 1. LOAD DATASET
    csv_path = "Retail_Transactions_Dataset.csv" # Ensure this matches your upload
    # Fallback to local path if running locally
    if not os.path.exists(csv_path):
        csv_path = os.path.join(DATA_DIR, 'Retail_Transactions_Dataset.csv')
        
    if os.path.exists(csv_path):
        print(f"📥 Ingesting {csv_path}...")
        df = pd.read_csv(csv_path)

        # 2. TRANSFORM: Explode the 'Product' list column
        # The file has "['A', 'B']" strings. We need real rows.
        print("   Parsing Product Lists...")
        df['Product_List'] = df['Product'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else [])
        
        # Create a simplified sales table (Exploded)
        sales_data = []
        unique_products = set()
        
        for _, row in df.iterrows():
            items = row['Product_List']
            if not items: continue
            
            # Smart Cost Allocation: Split total cost among items
            # (In a real scenario, we'd have unit prices, but this is a good estimate)
            item_price = row['Total_Cost'] / len(items)
            
            for item in items:
                unique_products.add(item)
                sales_data.append({
                    'transaction_id': row['Transaction_ID'],
                    'date_key': pd.to_datetime(row['Date']).strftime("%Y-%m-%d"),
                    'product_key': item,
                    'quantity': 1, # Each mention in the list is 1 item
                    'total_amount': item_price,
                    'city': row['City'],
                    'payment_method': row['Payment_Method'],
                    'customer_name': row['Customer_Name']
                })
        
        df_sales = pd.DataFrame(sales_data)
        
        # 3. LOAD DIMENSIONS
        print(f"   Loading {len(unique_products)} Unique Products...")
        pd.DataFrame({'product_key': list(unique_products), 'name': list(unique_products), 'category': 'General'}).to_sql('Dim_Product', conn, if_exists='append', index=False)
        
        # 4. LOAD FACT SALES
        print(f"   Loading {len(df_sales)} Sales Records...")
        df_sales.to_sql('Fact_Sales', conn, if_exists='append', index=False)

        # 5. GENERATE & LOAD OPERATIONS DATA (Since dataset is missing it)
        print("   Generating Inventory & Shipment Analytics...")
        
        # Inventory: Random stock for each product
        inventory_data = []
        for p in unique_products:
            inventory_data.append({
                'product_key': p,
                'stock_level': random.randint(0, 100),
                'turnover_ratio': round(random.uniform(1.0, 10.0), 2)
            })
        pd.DataFrame(inventory_data).to_sql('Fact_Inventory', conn, if_exists='append', index=False)

        # Shipments: Generate delivery times for transactions
        shipment_data = []
        unique_txns = df_sales['transaction_id'].unique()
        for txn in unique_txns:
            days = random.randint(1, 7)
            status = 'On Time' if days < 5 else 'Delayed'
            shipment_data.append({
                'transaction_id': txn,
                'delivery_days': days,
                'status': status
            })
        pd.DataFrame(shipment_data).to_sql('Fact_Shipments', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print("✅ ETL Complete: Dataset Processed & Missing Metrics Generated.")

if __name__ == "__main__":
    run_pipeline()