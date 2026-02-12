import pandas as pd
import json
import sqlite3
import os

# --- CONFIGURATION & PATHS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

print(f"🚀 Starting ETL. Database Path: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 1. CREATE TABLES (Star Schema)
cursor.execute('''CREATE TABLE IF NOT EXISTS Dim_Product (product_id TEXT PRIMARY KEY, name TEXT, category TEXT, unit_price REAL)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS Fact_Sales (id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id TEXT, source_system TEXT, product_id TEXT, quantity INTEGER, total_amount REAL, transaction_date TIMESTAMP, customer_city TEXT)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS Fact_Inventory (warehouse_id TEXT, product_id TEXT, stock_level INTEGER, last_updated TIMESTAMP)''')
conn.commit()

# 2. PROCESS POS SALES (CSV)
pos_path = os.path.join(DATA_DIR, 'pos_sales.csv')
if os.path.exists(pos_path):
    df_pos = pd.read_csv(pos_path)
    # Clean Negative Prices
    df_pos = df_pos[df_pos['total_amount'] > 0] 
    
    # Transform
    df_pos['source_system'] = 'POS'
    df_pos['customer_city'] = 'Unknown'
    df_pos_db = df_pos[['transaction_id', 'source_system', 'product_id', 'quantity', 'total_amount', 'timestamp', 'customer_city']]
    df_pos_db.columns = ['transaction_id', 'source_system', 'product_id', 'quantity', 'total_amount', 'transaction_date', 'customer_city']
    
    df_pos_db.to_sql('Fact_Sales', conn, if_exists='append', index=False)
    print("✅ POS Data Cleaned & Loaded")

# 3. PROCESS WEB ORDERS (JSON)
web_path = os.path.join(DATA_DIR, 'web_orders.json')
if os.path.exists(web_path):
    with open(web_path, 'r') as f:
        web_data = json.load(f)
    
    web_rows = []
    for order in web_data:
        for item in order['items']:
            web_rows.append({
                'transaction_id': order['order_id'],
                'source_system': 'WEB',
                'product_id': item['product_id'],
                'quantity': item['qty'],
                'total_amount': item['price'] * item['qty'],
                'transaction_date': order['order_date'],
                'customer_city': order['customer']['address']['city']
            })
    
    df_web = pd.DataFrame(web_rows)
    df_web.to_sql('Fact_Sales', conn, if_exists='append', index=False)
    print("✅ Web Data Parsed & Loaded")

# 4. PROCESS INVENTORY (CSV)
inv_path = os.path.join(DATA_DIR, 'warehouse_inventory.csv')
if os.path.exists(inv_path):
    df_inv = pd.read_csv(inv_path)
    
    # Quick fix: Populate Dim_Product with dummy names so foreign keys work
    unique_products = df_inv['product_id'].unique()
    for pid in unique_products:
        cursor.execute("SELECT 1 FROM Dim_Product WHERE product_id = ?", (pid,))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Dim_Product (product_id, name, category, unit_price) VALUES (?, ?, ?, ?)", (pid, f"Product {pid}", "General", 0.0))
            
    df_inv.to_sql('Fact_Inventory', conn, if_exists='replace', index=False)
    print("✅ Inventory Loaded")

conn.commit()
conn.close()
print("🎉 ETL Complete.")   