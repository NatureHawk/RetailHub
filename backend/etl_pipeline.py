import pandas as pd
import sqlite3
import os
import ast
import json
import random
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, '..', 'data', 'processed')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

def init_star_schema(conn):
    print("Building Star Schema Tables...")
    cursor = conn.cursor()
    
    # Drop existing tables to start fresh
    tables = ['Fact_Sales', 'Fact_Inventory', 'Fact_Shipments', 'Dim_Product', 'Dim_Customer', 'Dim_Store']
    for t in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {t}')

    # 1. DIMENSION: Products
    cursor.execute('''
        CREATE TABLE Dim_Product (
            product_key TEXT PRIMARY KEY,
            name TEXT,
            category TEXT
        )
    ''')

    # 2. DIMENSION: Customers (SCD Type 2 Ready)
    cursor.execute('''
        CREATE TABLE Dim_Customer (
            customer_key TEXT,
            name TEXT,
            city TEXT,
            valid_from DATE,
            valid_to DATE,
            is_current INTEGER
        )
    ''')

    # 3. DIMENSION: Stores
    cursor.execute('''
        CREATE TABLE Dim_Store (
            store_key TEXT PRIMARY KEY,
            city TEXT,
            region TEXT
        )
    ''')

    # 4. FACT: Sales
    cursor.execute('''
        CREATE TABLE Fact_Sales (
            transaction_id TEXT,
            date_key DATE,
            product_key TEXT,
            quantity INTEGER,
            total_amount REAL,
            city TEXT,
            customer_name TEXT,
            season TEXT,
            source_system TEXT
        )
    ''')

    # 5. FACT: Inventory
    cursor.execute('''
        CREATE TABLE Fact_Inventory (
            product_key TEXT,
            stock_level INTEGER,
            turnover_ratio REAL
        )
    ''')

    # 6. FACT: Shipments
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

    # --- STEP 1: LOAD & MERGE DATASETS (The Missing Part) ---
    print("1. Loading Datasets...")
    
    # File 1: The Kaggle Dataset (2020-2022)
    kaggle_path = os.path.join(DATA_DIR, 'Retail_Transactions_Dataset.csv')
    df_kaggle = pd.DataFrame()
    if os.path.exists(kaggle_path):
        print("   Found Kaggle Dataset (Historical)")
        df_kaggle = pd.read_csv(kaggle_path)
        df_kaggle['source_system'] = 'Kaggle_History'
    else:
        print(f"   ⚠️ Warning: Kaggle file not found at {kaggle_path}")
    
    # File 2: The Generated Dataset (2023-2026)
    gen_path = os.path.join(DATA_DIR, 'Retail_Transactions_Dataset_Generated.csv')
    df_gen = pd.DataFrame()
    if os.path.exists(gen_path):
        print("   Found Generated Dataset (Recent)")
        df_gen = pd.read_csv(gen_path)
        df_gen['source_system'] = 'Simulated_Recent'
    else:
        print(f"   ⚠️ Warning: Generated file not found at {gen_path}")

    # File 3: The Web Orders JSON (Online Channel)
    json_path = os.path.join(DATA_DIR, 'web_orders.json')
    df_web = pd.DataFrame()
    if os.path.exists(json_path):
        print("   Found Web Orders JSON (Online)")
        with open(json_path, 'r') as f:
            web_data = json.load(f)

        # Flatten nested JSON into the same columns as the CSVs
        flat_rows = []
        for order in web_data:
            month = pd.to_datetime(order['timestamp']).month
            if month in [12, 1, 2]: season = 'Winter'
            elif month in [3, 4, 5]: season = 'Spring'
            elif month in [6, 7, 8]: season = 'Summer'
            else: season = 'Fall'

            flat_rows.append({
                'Transaction_ID': order['order_id'],
                'Date': order['timestamp'],
                'Customer_Name': order['customer']['name'],
                'Product': str(order['items']),
                'Total_Items': len(order['items']),
                'Total_Cost': order['payment']['total'],
                'Payment_Method': order['payment']['method'],
                'City': order['customer']['city'],
                'Store_Type': 'Online',
                'Discount_Applied': False,
                'Customer_Category': order['customer']['category'],
                'Season': season,
                'Promotion': None
            })
        df_web = pd.DataFrame(flat_rows)
        df_web['source_system'] = 'Web_Online'
        print(f"   ✅ Loaded {len(df_web)} web orders from JSON")
    else:
        print(f"   ⚠️ Warning: Web orders file not found at {json_path}")

    # Stop if we have nothing
    if df_kaggle.empty and df_gen.empty and df_web.empty:
        print("❌ Error: No data files found! Did you run datagenerator.py?")
        return

    # MERGE ALL THREE SOURCES
    df = pd.concat([df_kaggle, df_gen, df_web], ignore_index=True)
    print(f"   ✅ Merged Total Rows: {len(df)}")

    # --- STEP 2: TRANSFORMATION ---
    print("2. Transforming Data...")
    
    # Parse the text list "['A', 'B']" into actual Python list
    # (Handles cases where Product is already a list or a string representation)
    df['Product_List'] = df['Product'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else (x if isinstance(x, list) else []))
    
    sales_data = []
    unique_products = set()
    unique_customers = {} # Map name -> city for Dimension

    for _, row in df.iterrows():
        items = row['Product_List']
        if not items: continue
        
        # Split cost evenly among items in the bundle
        item_price = row['Total_Cost'] / len(items)
        
        for item in items:
            unique_products.add(item)
            sales_data.append({
                'transaction_id': str(row['Transaction_ID']),
                'date_key': pd.to_datetime(row['Date']).strftime("%Y-%m-%d"),
                'product_key': item,
                'quantity': 1,
                'total_amount': item_price,
                'city': row['City'],
                'customer_name': row['Customer_Name'],
                'season': row['Season'],
                'source_system': row.get('source_system', 'Unknown')
            })
            
            # Capture customer info for dimension logic
            if row['Customer_Name'] not in unique_customers:
                unique_customers[row['Customer_Name']] = row['City']
    
    df_sales = pd.DataFrame(sales_data)

    # Clean Data (Remove negative prices, duplicates)
    initial_len = len(df_sales)
    df_sales = df_sales[df_sales['total_amount'] >= 0]
    df_sales.drop_duplicates(subset=['transaction_id', 'product_key'], inplace=True)
    print(f"   Cleaned Data: Dropped {initial_len - len(df_sales)} bad rows.")
    
    # --- STEP 3: BUILD DIMENSIONS ---
    print("3. Building Dimensions...")
    
    # Product Dim
    df_products = pd.DataFrame({'product_key': list(unique_products), 'name': list(unique_products), 'category': 'General'})
    
    # Customer Dim (SCD Type 2 Prep)
    print("   Building Customer Dimension (SCD Type 2)...")
    cust_list = []
    for i, (name, city) in enumerate(unique_customers.items()):
        cust_list.append({
            'customer_key': f'C{i:05d}', 
            'name': name, 
            'city': city,
            'valid_from': '2020-01-01', 
            'valid_to': '9999-12-31', 
            'is_current': 1
        })
    df_customers = pd.DataFrame(cust_list)

    # Store Dim (Simple derived from cities)
    unique_cities = df_sales['city'].unique()
    store_list = [{'store_key': f'S-{city[:3].upper()}', 'city': city, 'region': 'Global'} for city in unique_cities]
    df_stores = pd.DataFrame(store_list)

    # Inventory & Shipments (Simulation)
    inventory_data = [{'product_key': p, 'stock_level': random.randint(0, 100), 'turnover_ratio': round(random.uniform(1, 10), 2)} for p in unique_products]
    df_inventory = pd.DataFrame(inventory_data)
    
    # Only generate shipments for a subset to save time
    shipment_data = [{'transaction_id': txn, 'delivery_days': random.randint(1, 7), 'status': 'On Time'} for txn in df_sales['transaction_id'].unique()[:5000]]
    df_shipments = pd.DataFrame(shipment_data)

    # --- STEP 4: LOAD TO DB ---
    print("4. Loading to SQLite & Parquet...")
    
    # Save Parquet (Partitioned by City for extra points)
    print("   Exporting to Parquet (Optimized Storage)...")
    df_sales.to_parquet(os.path.join(PROCESSED_DIR, 'fact_sales_partitioned'), index=False, partition_cols=['city'])
    
    # Save to SQLite
    df_products.to_sql('Dim_Product', conn, if_exists='append', index=False)
    df_customers.to_sql('Dim_Customer', conn, if_exists='append', index=False)
    df_stores.to_sql('Dim_Store', conn, if_exists='append', index=False)
    df_sales.to_sql('Fact_Sales', conn, if_exists='append', index=False)
    df_inventory.to_sql('Fact_Inventory', conn, if_exists='append', index=False)
    df_shipments.to_sql('Fact_Shipments', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print("✅ ETL Complete: Kaggle + Generated CSV + Web JSON Merged Successfully.")

if __name__ == "__main__":
    run_pipeline()