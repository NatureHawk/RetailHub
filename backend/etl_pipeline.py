import pandas as pd
import json
import sqlite3
import os
import time

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, '..', 'data', 'processed')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

os.makedirs(PROCESSED_DIR, exist_ok=True)

print(f"🚀 Starting Enterprise ETL Pipeline...")

# --- 1. THE CLEANING ENGINE (Requirement 2B) ---
def clean_and_transform(df, source_name):
    print(f"\n🧼 Cleaning Data from: {source_name}")
    initial_count = len(df)

    # A. Remove Duplicates (Logic: Same Transaction ID = Duplicate)
    if 'transaction_id' in df.columns:
        df.drop_duplicates(subset=['transaction_id'], keep='first', inplace=True)
    duplicates_removed = initial_count - len(df)

    # B. Fix Missing Values (Logic: Fill text with 'Unknown', numbers with 0)
    # This prevents the dashboard from showing "NaN" or crashing
    cols_fixed = []
    for col in df.columns:
        if df[col].dtype == 'object': # Text columns
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna('Unknown')
                cols_fixed.append(col)
        else: # Number columns
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna(0)
                cols_fixed.append(col)

    # C. Ensure Data Logic (Logic: Price cannot be negative)
    # We convert negative prices to positive (assuming it was a typo) 
    # OR we drop them. Let's drop them to be strict as per requirements.
    bad_price_count = 0
    if 'total_amount' in df.columns:
        bad_price_count = len(df[df['total_amount'] < 0])
        df = df[df['total_amount'] >= 0]

    # REPORTING (Show this in your demo!)
    print(f"   - ✂️ Duplicates Removed: {duplicates_removed}")
    print(f"   - 🩹 Missing Values Fixed in: {cols_fixed}")
    print(f"   - 🚫 Negative Prices Dropped: {bad_price_count}")
    print(f"   - ✅ Final Row Count: {len(df)}")
    
    return df

# --- 2. SMART SCHEMA EVOLUTION ---
def align_schema(df, table_name, conn):
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        existing_cols = [row[1] for row in cursor.fetchall()]
        
        for col in df.columns:
            if col not in existing_cols:
                print(f"   ⚠️ Schema Evolution: Adding new column '{col}' to {table_name}")
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
    except Exception as e:
        print(f"   Note: Table might not exist yet. {e}")

# --- 3. MAIN PIPELINE ---
def run_pipeline():
    conn = sqlite3.connect(DB_PATH)
    
    # Ingest POS (CSV)
    pos_path = os.path.join(DATA_DIR, 'pos_sales.csv')
    if os.path.exists(pos_path):
        print("📥 Reading POS Data...")
        df = pd.read_csv(pos_path)
        
        # CLEAN IT
        df = clean_and_transform(df, "POS System")
        
        # TRANSFORM IT
        df['source'] = 'POS'
        if 'timestamp' in df.columns:
            df.rename(columns={'timestamp': 'transaction_date'}, inplace=True)

        align_schema(df, 'Fact_Sales', conn)
        df.to_sql('Fact_Sales', conn, if_exists='append', index=False)

    # Ingest Web (JSON)
    web_path = os.path.join(DATA_DIR, 'web_orders.json')
    if os.path.exists(web_path):
        print("📥 Reading Web Data...")
        with open(web_path) as f:
            data = json.load(f)
            
        web_rows = []
        for order in data:
            for item in order['items']:
                web_rows.append({
                    'transaction_id': order['order_id'],
                    'source': 'WEB',
                    'product_id': item['product_id'],
                    'quantity': item['qty'],
                    'total_amount': item['qty'] * item['price'],
                    'transaction_date': order['order_date'],
                    'city': order['customer'].get('city', 'Unknown')
                })
        
        df_web = pd.DataFrame(web_rows)
        
        # CLEAN IT
        df_web = clean_and_transform(df_web, "Web Orders")
        
        align_schema(df_web, 'Fact_Sales', conn)
        df_web.to_sql('Fact_Sales', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print("\n🎉 ETL Complete: Data is Clean, Logical, and Stored.")

if __name__ == "__main__":
    run_pipeline()