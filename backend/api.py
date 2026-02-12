from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import os
import random
from datetime import datetime

app = FastAPI()

# --- SECURITY CONFIGURATION ---
API_SECRET_KEY = "retail-hackathon-secure-key-123"

# Dependency: This runs before every request to check the key
async def verify_key(x_api_key: str = Header(None)):
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="⛔ Unauthorized: Invalid API Key")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw', 'retail_data_hub.db')

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

# --- SECURED ENDPOINTS (Require API Key) ---

@app.post("/ingest/order", dependencies=[Depends(verify_key)]) # <--- LOCKED
def ingest_realtime_order(order: Order):
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO Fact_Sales (transaction_id, source, product_id, quantity, total_amount, transaction_date, city)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (order.transaction_id, order.source, order.product_id, order.quantity, order.total_amount, datetime.now(), order.city))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/kpi/summary") # Public (for Dashboard demo)
def get_kpi_summary():
    conn = get_db()
    rev = conn.execute("SELECT SUM(total_amount) as val FROM Fact_Sales").fetchone()['val']
    orders = conn.execute("SELECT COUNT(*) as val FROM Fact_Sales").fetchone()['val']
    clv = conn.execute("SELECT SUM(total_amount) * 1.0 / COUNT(DISTINCT transaction_id) as val FROM Fact_Sales WHERE source = 'WEB'").fetchone()['val']
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

# Keep Simulation Public for easy Demo
@app.post("/simulate/new-sale")
def simulate_sale():
    conn = get_db()
    conn.execute("INSERT INTO Fact_Sales (transaction_id, source, total_amount, transaction_date) VALUES (?, 'POS-SIM', ?, date('now'))", 
                 (str(random.randint(10000,99999)), random.uniform(50, 200)))
    conn.commit()
    return {"msg": "Simulation Added"}