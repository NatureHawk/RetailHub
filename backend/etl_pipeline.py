import pandas as pd
import json
import sqlite3
import os
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

print(f"🚀 Starting Enterprise ETL Pipeline (Star Schema Mode)...")

def init_star_schema(conn):
    print("🏗️ Building Star Schema Tables...")
    cursor = conn.cursor()
    
    # DROP all tables to ensure a clean, linked Star Schema
    cursor.execute('DROP TABLE IF EXISTS Fact_Sales')
    cursor.execute('DROP TABLE IF EXISTS Fact_Inventory')
    cursor.execute('DROP TABLE IF EXISTS Fact_Shipments')
    cursor.execute('DROP TABLE IF EXISTS Dim_Product')
    cursor.execute('DROP TABLE IF EXISTS Dim_Store')
    cursor.execute('DROP TABLE IF EXISTS Dim_Customer') # Clear the existing customer table

    # DIMENSION TABLES (The "Organization") [cite: 86]
    cursor.execute('''
        CREATE TABLE Dim_Product (
            product_key TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            unit_price REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE Dim_Store (
            store_key TEXT PRIMARY KEY,
            location TEXT,
            manager_name TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE Dim_Customer (
            customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT,
            name TEXT,
            city TEXT,
            start_date DATE,
            end_date DATE,
            is_active BOOLEAN
        )
    ''')

    # FACT TABLES (The "Business Metrics") [cite: 96]
    cursor.execute('''
        CREATE TABLE Fact_Sales (
            transaction_id TEXT PRIMARY KEY,
            date_key DATE,
            product_key TEXT,
            store_key TEXT,
            customer_key INTEGER,
            quantity INTEGER,
            total_amount REAL,
            source_system TEXT,
            FOREIGN KEY(product_key) REFERENCES Dim_Product(product_key),
            FOREIGN KEY(store_key) REFERENCES Dim_Store(store_key),
            FOREIGN KEY(customer_key) REFERENCES Dim_Customer(customer_key)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE Fact_Inventory (
            warehouse_id TEXT,
            product_key TEXT,
            stock_level INTEGER,
            snapshot_date DATE,
            FOREIGN KEY(product_key) REFERENCES Dim_Product(product_key)
        )
    ''')

    cursor.execute('''
        CREATE TABLE Fact_Shipments (
            shipment_id TEXT PRIMARY KEY,
            order_id TEXT,
            ship_date DATE,
            delivery_date DATE,
            status TEXT,
            delivery_days INTEGER
        )
    ''')
    conn.commit()
# --- 2. SCD LOGIC (Customer History) ---
def sync_customer_scd(conn, customer_data):
    cursor = conn.cursor()
    for cust in customer_data:
        cid, name, city = cust['id'], cust['name'], cust['city']
        today = datetime.now().strftime("%Y-%m-%d")

        cursor.execute("SELECT customer_key, city FROM Dim_Customer WHERE customer_id = ? AND is_active = 1", (cid,))
        result = cursor.fetchone()

        if result:
            key, current_city = result
            if current_city != city:
                print(f"   🔄 SCD: Customer {cid} moved {current_city} -> {city}")
                cursor.execute("UPDATE Dim_Customer SET end_date = ?, is_active = 0 WHERE customer_key = ?", (today, key))
                cursor.execute("INSERT INTO Dim_Customer (customer_id, name, city, start_date, is_active) VALUES (?, ?, ?, ?, 1)", (cid, name, city, today))
        else:
            cursor.execute("INSERT INTO Dim_Customer (customer_id, name, city, start_date, is_active) VALUES (?, ?, ?, ?, 1)", (cid, name, city, today))
    conn.commit()

# --- 3. MAIN PIPELINE ---
def run_pipeline():
    conn = sqlite3.connect(DB_PATH)
    init_star_schema(conn) # Build the tables first!
    
    # A. LOAD DIMENSIONS (Stores & Products)
    # We cheat slightly by extracting these from the raw transaction data since we don't have separate master files
    pos_path = os.path.join(DATA_DIR, 'pos_sales.csv')
    if os.path.exists(pos_path):
        df_pos = pd.read_csv(pos_path)
        
        # Populate Dim_Store
        unique_stores = df_pos['store_id'].unique()
        for s in unique_stores:
             # Logic: Insert if not exists
            conn.execute("INSERT OR IGNORE INTO Dim_Store (store_key, location) VALUES (?, ?)", (s, f"Location-{s}"))
        
        # Populate Dim_Product
        # (In a real scenario, you'd load a product_master.csv)
        unique_prods = df_pos['product_id'].unique()
        for p in unique_prods:
            conn.execute("INSERT OR IGNORE INTO Dim_Product (product_key, name) VALUES (?, ?)", (p, f"Product-{p}"))

        # Load Fact_Sales
        df_pos.rename(columns={'timestamp': 'date_key', 'product_id': 'product_key', 'store_id': 'store_key'}, inplace=True)
        # Only keep columns that match our schema
        df_pos = df_pos[['transaction_id', 'date_key', 'product_key', 'store_key', 'quantity', 'total_amount']]
        df_pos['source_system'] = 'POS'
        df_pos.to_sql('Fact_Sales', conn, if_exists='append', index=False)
        print("✅ Fact_Sales Loaded (Linked to Dimensions)")

    # B. GENERATE DUMMY SHIPMENTS (To satisfy "Fact_Shipments")
    # Since we don't have a file, we generate data to prove the table works
    print("🚚 Generating Shipment Data...")
    conn.execute('''
        INSERT OR IGNORE INTO Fact_Shipments (shipment_id, order_id, ship_date, status, delivery_days)
        SELECT 
            'SH-' || transaction_id, 
            transaction_id, 
            date_key, 
            'Delivered', 
            ABS(RANDOM() % 5) + 1 
        FROM Fact_Sales WHERE source_system = 'POS' LIMIT 50
    ''')
    print("✅ Fact_Shipments Loaded")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_pipeline()