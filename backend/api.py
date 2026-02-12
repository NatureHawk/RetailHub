from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os
import random
from datetime import datetime

app = FastAPI()

# Enable CORS (Allows your Flutter app to talk to this server)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw', 'retail_data_hub.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn

@app.get("/")
def read_root():
    return {"status": "Online", "message": "Smart Retail Data Hub API is running!"}

# --- 1. KPI ENDPOINT (For the Dashboard Cards) ---
@app.get("/kpi/summary")
def get_kpi_summary():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Calculate Total Revenue
    cursor.execute("SELECT SUM(total_amount) as revenue FROM Fact_Sales")
    total_revenue = cursor.fetchone()['revenue'] or 0
    
    # Calculate Total Orders
    cursor.execute("SELECT COUNT(*) as orders FROM Fact_Sales")
    total_orders = cursor.fetchone()['orders']
    
    # Calculate Low Stock Items (< 20 units)
    cursor.execute("SELECT COUNT(*) as low_stock FROM Fact_Inventory WHERE stock_level < 20")
    low_stock = cursor.fetchone()['low_stock']
    
    conn.close()
    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "low_stock_alerts": low_stock
    }

# --- 2. CHART ENDPOINT (For the Revenue Graph) ---
@app.get("/kpi/revenue-trend")
def get_revenue_trend():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get sales grouped by date (Limit to last 7 days for the chart)
    cursor.execute("""
        SELECT date(transaction_date) as day, SUM(total_amount) as daily_revenue 
        FROM Fact_Sales 
        GROUP BY day 
        ORDER BY day DESC 
        LIMIT 7
    """)
    rows = cursor.fetchall()
    conn.close()
    
    # Format for Flutter (List of objects)
    return [{"day": row['day'], "revenue": row['daily_revenue']} for row in rows]

# --- 3. INVENTORY ENDPOINT (For the 'Stock' Tab) ---
@app.get("/inventory/low-stock")
def get_low_stock_items():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get specific products that are running low
    cursor.execute("""
        SELECT p.name, i.stock_level, i.warehouse_id 
        FROM Fact_Inventory i
        JOIN Dim_Product p ON i.product_id = p.product_id
        WHERE i.stock_level < 20
        ORDER BY i.stock_level ASC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    conn.close()
    
    return [{"product": row['name'], "stock": row['stock_level'], "warehouse": row['warehouse_id']} for row in rows]

# --- 4. REAL-TIME SIMULATION (The "Magic Button") ---
@app.post("/simulate/new-sale")
def simulate_sale():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Insert a random new sale into the DB
    random_amount = round(random.uniform(50, 500), 2)
    cursor.execute("""
        INSERT INTO Fact_Sales (transaction_id, source_system, product_id, quantity, total_amount, transaction_date, customer_city)
        VALUES (?, 'REAL-TIME-POS', 'P001', 1, ?, ?, 'Mumbai')
    """, (f"RT-{random.randint(1000,9999)}", random_amount, datetime.now()))
    
    conn.commit()
    conn.close()
    return {"message": "New sale simulated!", "amount": random_amount}