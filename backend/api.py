from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw', 'retail_data_hub.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# --- TAB 1: COMMERCIAL ANALYTICS ---
@app.get("/analytics/commercial")
def get_commercial_data():
    conn = get_db()
    
    # 1. Monthly Revenue
    rev_query = """
        SELECT strftime('%Y-%m', date_key) as month, SUM(total_amount) as revenue 
        FROM Fact_Sales GROUP BY month ORDER BY month DESC LIMIT 12
    """
    revenue = conn.execute(rev_query).fetchall()
    
    # 2. Top Selling Products
    top_prod_query = """
        SELECT product_key, COUNT(*) as units_sold 
        FROM Fact_Sales GROUP BY product_key ORDER BY units_sold DESC LIMIT 5
    """
    top_products = conn.execute(top_prod_query).fetchall()
    
    # 3. City-wise Sales
    city_query = """
        SELECT city, SUM(total_amount) as revenue 
        FROM Fact_Sales GROUP BY city ORDER BY revenue DESC LIMIT 5
    """
    city_sales = conn.execute(city_query).fetchall()
    
    return {
        "monthly_revenue": [{"month": r['month'], "revenue": r['revenue']} for r in revenue],
        "top_products": [{"name": r['product_key'], "sold": r['units_sold']} for r in top_products],
        "city_sales": [{"city": r['city'], "revenue": r['revenue']} for r in city_sales]
    }

# --- TAB 2: OPERATIONS ANALYTICS ---
@app.get("/analytics/operations")
def get_operations_data():
    conn = get_db()
    
    # 1. Inventory Turnover (Avg of generated data)
    inv_query = "SELECT AVG(turnover_ratio) as val, SUM(stock_level) as total_stock FROM Fact_Inventory"
    inv = conn.execute(inv_query).fetchone()
    
    # 2. Avg Delivery Time
    del_query = "SELECT AVG(delivery_days) as val FROM Fact_Shipments"
    delivery = conn.execute(del_query).fetchone()
    
    return {
        "inventory_turnover": round(inv['val'] or 0, 2),
        "total_stock": inv['total_stock'] or 0,
        "avg_delivery_days": round(delivery['val'] or 0, 1)
    }

# --- TAB 3: CUSTOMER ANALYTICS ---
@app.get("/analytics/customer")
def get_customer_data():
    conn = get_db()
    
    # 1. New vs Returning (Simplified logic: Transactions > 1)
    # This requires grouping by customer first
    ret_query = """
        SELECT 
            CASE WHEN cnt > 1 THEN 'Returning' ELSE 'New' END as type,
            COUNT(*) as count
        FROM (SELECT customer_name, COUNT(*) as cnt FROM Fact_Sales GROUP BY customer_name)
        GROUP BY type
    """
    shoppers = conn.execute(ret_query).fetchall()
    
    # 2. Customer Lifetime Value (Top 5)
    clv_query = """
        SELECT customer_name, SUM(total_amount) as clv 
        FROM Fact_Sales GROUP BY customer_name ORDER BY clv DESC LIMIT 5
    """
    clv = conn.execute(clv_query).fetchall()
    
    # 3. Market Basket Analysis (Simplified Pairs)
    # Using the exploded Fact_Sales to find products in same transaction
    basket_query = """
        SELECT A.product_key as item1, B.product_key as item2, COUNT(*) as frequency
        FROM Fact_Sales A
        JOIN Fact_Sales B ON A.transaction_id = B.transaction_id
        WHERE A.product_key < B.product_key
        GROUP BY item1, item2
        ORDER BY frequency DESC LIMIT 5
    """
    basket = conn.execute(basket_query).fetchall()
    
    return {
        "shopper_type": [{"type": r['type'], "count": r['count']} for r in shoppers],
        "top_clv": [{"customer": r['customer_name'], "value": r['clv']} for r in clv],
        "market_basket": [{"pair": f"{r['item1']} + {r['item2']}", "count": r['frequency']} for r in basket]
    }