from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel # New library for data validation
import sqlite3
import os
import random
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw', 'retail_data_hub.db')

# VALIDATION MODEL (Ensures incoming data is correct)
class Order(BaseModel):
    transaction_id: str
    source: str
    product_id: str
    quantity: int
    total_amount: float
    city: str

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# --- 1. REAL-TIME INGESTION ENDPOINT (The "Pipeline" Target) ---
@app.post("/ingest/order")
def ingest_realtime_order(order: Order):
    """
    Receives live data from stores/web and saves it instantly.
    Satisfies: "Near Real-Time (instant updates)" requirement.
    """
    try:
        conn = get_db()
        # Insert immediately into the Fact Table
        conn.execute("""
            INSERT INTO Fact_Sales (transaction_id, source, product_id, quantity, total_amount, transaction_date, city)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (order.transaction_id, order.source, order.product_id, order.quantity, order.total_amount, datetime.now(), order.city))
        conn.commit()
        conn.close()
        return {"status": "success", "msg": "Data Ingested Real-Time"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. EXISTING KPI ENDPOINTS (unchanged) ---
@app.get("/kpi/summary")
def get_kpi_summary():
    conn = get_db()
    rev = conn.execute("SELECT SUM(total_amount) as val FROM Fact_Sales").fetchone()['val']
    orders = conn.execute("SELECT COUNT(*) as val FROM Fact_Sales").fetchone()['val']
    
    # CLV Logic
    clv_query = "SELECT SUM(total_amount) * 1.0 / COUNT(DISTINCT transaction_id) as val FROM Fact_Sales WHERE source = 'WEB'"
    clv = conn.execute(clv_query).fetchone()['val']
    
    stock = conn.execute("SELECT COUNT(*) as val FROM Fact_Inventory WHERE stock_level < 10").fetchone()['val']
    
    return {
        "total_revenue": round(rev or 0, 2),
        "total_orders": orders,
        "avg_order_value": round(clv or 0, 2),
        "low_stock_alerts": stock
    }

@app.get("/kpi/revenue-trend")
def get_revenue_trend():
    conn = get_db()
    rows = conn.execute("SELECT substr(transaction_date, 1, 10) as day, SUM(total_amount) as revenue FROM Fact_Sales GROUP BY day ORDER BY day DESC LIMIT 7").fetchall()
    return [{"day": r['day'], "revenue": r['revenue']} for r in rows]

# --- 3. SIMULATION ENDPOINT (For your Demo Button) ---
# We keep this so your "Magic Button" still works for the video
@app.post("/simulate/new-sale")
def simulate_sale():
    conn = get_db()
    conn.execute("INSERT INTO Fact_Sales (transaction_id, source, total_amount, transaction_date) VALUES (?, 'POS-SIM', ?, date('now'))", 
                 (str(random.randint(10000,99999)), random.uniform(50, 200)))
    conn.commit()
    return {"msg": "Simulation Added"}