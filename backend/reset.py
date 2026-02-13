import os
import shutil

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, '..', 'data', 'processed')
DB_PATH = os.path.join(DATA_DIR, 'retail_data_hub.db')

def wipe_files():
    print("🧹 STARTING PROJECT CLEANUP...")

    # 1. DELETE THE DATABASE (The most important part)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"   🗑️  Deleted Database: {DB_PATH}")
    else:
        print(f"   ✓  Database already clean.")

    # 2. DELETE OLD GENERATED FILES (The "Bad" 2026 Data)
    # These are the files from your OLD datagenerator
    bad_files = [
        'pos_sales.csv', 
        'web_orders.json', 
        'warehouse_inventory.csv',
        'Retail_Transactions_Dataset_Generated.csv' # Delete this to force a fresh generation
    ]

    for f in bad_files:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            os.remove(path)
            print(f"   🗑️  Deleted Old Data: {f}")

    # 3. DELETE PROCESSED PARQUET FILES
    if os.path.exists(PROCESSED_DIR):
        shutil.rmtree(PROCESSED_DIR)
        print(f"   🗑️  Deleted Processed Parquet Folder")

    print("✨ CLEANUP COMPLETE. You have a blank slate.")

if __name__ == "__main__":
    wipe_files()