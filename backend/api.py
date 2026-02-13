from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import os
import random
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- SECURITY ---
API_SECRET_KEY = "retail-hackathon-secure-key-123"

async def verify_key(x_api_key: str = Header(None)):
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid API Key")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw', 'retail_data_hub.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# --- UPDATED DATA MODEL ---
class Order(BaseModel):
    transaction_id: str
    source: str
    product_id: str
    quantity: int
    total_amount: float
    city: str
    customer_name: str
    season: str

# --- 1. REAL-TIME INGESTION ---
@app.post("/ingest/order", dependencies=[Depends(verify_key)])
def ingest_realtime_order(order: Order):
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO Fact_Sales (
                transaction_id, date_key, product_key, quantity, total_amount, 
                city, source_system, customer_name, season
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            order.transaction_id, 
            datetime.now().strftime("%Y-%m-%d"), 
            order.product_id, 
            order.quantity, 
            order.total_amount, 
            order.city, 
            order.source,
            order.customer_name,
            order.season
        ))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. DEMO SIMULATION ---
@app.post("/simulate/new-sale")
def simulate_sale():
    conn = get_db()
    seasons = ['Winter', 'Spring', 'Summer', 'Fall']
    # Added random city selection so filters work better
    cities = ['New York', 'London', 'Mumbai', 'Demo City'] 
    conn.execute("""
        INSERT INTO Fact_Sales (
            transaction_id, date_key, product_key, quantity, total_amount, 
            source_system, season, customer_name, city
        )
        VALUES (?, date('now'), 'Simulated Product', 1, ?, 'SIMULATOR', ?, 'Demo User', ?)
    """, (
        str(random.randint(1000,9999)), 
        random.uniform(20, 100), 
        random.choice(seasons),
        random.choice(cities)
    ))
    conn.commit()
    return {"msg": "Sale Simulated"}


# --- 4. ANALYTICS TABS (EXISTING) ---

@app.get("/analytics/commercial")
def get_commercial_data():
    conn = get_db()
    rev = conn.execute("SELECT strftime('%Y-%m', date_key) as month, SUM(total_amount) as revenue FROM Fact_Sales GROUP BY month ORDER BY month DESC LIMIT 12").fetchall()
    prod = conn.execute("SELECT product_key, COUNT(*) as units FROM Fact_Sales GROUP BY product_key ORDER BY units DESC LIMIT 5").fetchall()
    return {
        "monthly_revenue": [{"month": r['month'], "revenue": r['revenue']} for r in rev],
        "top_products": [{"name": r['product_key'], "sold": r['units']} for r in prod]
    }

@app.get("/analytics/operations")
def get_operations_data():
    conn = get_db()

    # --- Categorization Helper (works around Dim_Product having 'General') ---
    CATEGORY_MAP = {
        'Banana': 'Food', 'Milk': 'Food', 'Chips': 'Food', 'Apple': 'Food',
        'Beef': 'Food', 'Ice Cream': 'Food', 'Tuna': 'Food', 'Eggs': 'Food',
        'Chicken': 'Food', 'Rice': 'Food', 'Pasta': 'Food', 'Bread': 'Food',
        'Cheese': 'Food', 'Yogurt': 'Food', 'Butter': 'Food', 'Coffee': 'Food',
        'Tea': 'Food', 'Sugar': 'Food', 'Cereal Bars': 'Food', 'Honey': 'Food',
        'Potatoes': 'Food', 'Onions': 'Food', 'Carrots': 'Food', 'Orange': 'Food',
        'Soda': 'Food', 'Water': 'Food', 'Peanut Butter': 'Food', 'Jam': 'Food',
        'Ketchup': 'Food', 'BBQ Sauce': 'Food', 'Vinegar': 'Food', 'Pickles': 'Food',
        'Syrup': 'Food', 'Salt': 'Food', 'Oil': 'Food', 'Flour': 'Food',
        'Canned Soup': 'Food',
        'Detergent': 'Home', 'Trash Bags': 'Home', 'Trash Cans': 'Home',
        'Sponges': 'Home', 'Mop': 'Home', 'Tissues': 'Home',
        'Shampoo': 'Personal Care', 'Soap': 'Personal Care', 'Toothpaste': 'Personal Care',
        'Shower Gel': 'Personal Care', 'Hair Gel': 'Personal Care',
        'Shaving Cream': 'Personal Care', 'Hand Sanitizer': 'Personal Care',
    }

    # Fetch all sales with product + season
    rows = conn.execute("SELECT product_key, season FROM Fact_Sales WHERE season IS NOT NULL").fetchall()

    # Group by season + category
    from collections import defaultdict
    season_cat = defaultdict(lambda: defaultdict(int))
    for r in rows:
        cat = CATEGORY_MAP.get(r['product_key'], 'Other')
        season_cat[r['season']][cat] += 1

    # Build stacked bar data
    season_order = ['Winter', 'Spring', 'Summer', 'Fall']
    seasonal_by_category = []
    for season in season_order:
        entry = {"name": season}
        entry.update(season_cat.get(season, {}))
        seasonal_by_category.append(entry)

    # Keep existing simple seasonal data for backward compat
    seasonal = conn.execute("SELECT season, COUNT(*) as sales_count FROM Fact_Sales WHERE season IS NOT NULL GROUP BY season ORDER BY sales_count DESC").fetchall()
    inv = conn.execute("SELECT AVG(turnover_ratio) as val FROM Fact_Inventory").fetchone()
    delivery = conn.execute("SELECT AVG(delivery_days) as val FROM Fact_Shipments").fetchone()

    return {
        "seasonal_trends": [{"season": r['season'], "sales": r['sales_count']} for r in seasonal],
        "seasonal_by_category": seasonal_by_category,
        "inventory_turnover": round(inv['val'] or 0, 2),
        "avg_delivery_days": round(delivery['val'] or 0, 1)
    }

@app.get("/analytics/customer")
def get_customer_data():
    conn = get_db()

    # --- Retention: New vs Returning customers ---
    total_customers = conn.execute("SELECT COUNT(DISTINCT customer_name) FROM Fact_Sales").fetchone()[0] or 1
    returning = conn.execute("SELECT COUNT(*) FROM (SELECT customer_name FROM Fact_Sales GROUP BY customer_name HAVING COUNT(DISTINCT transaction_id) > 1)").fetchone()[0]
    new_customers = total_customers - returning
    retention = [
        {"name": "New", "value": new_customers},
        {"name": "Returning", "value": returning}
    ]

    # --- CLV Trend: average customer lifetime value by month ---
    clv_rows = conn.execute("""
        SELECT strftime('%Y-%m', date_key) as month, ROUND(AVG(total_amount), 2) as value
        FROM Fact_Sales
        WHERE date_key IS NOT NULL
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
    """).fetchall()
    clv_trend = list(reversed([{"month": r["month"], "value": r["value"]} for r in clv_rows]))

    # --- Market Basket: frequently bought together ---
    basket = conn.execute("""
        SELECT A.product_key as item1, B.product_key as item2, COUNT(*) as frequency
        FROM Fact_Sales A
        JOIN Fact_Sales B ON A.transaction_id = B.transaction_id
        WHERE A.product_key < B.product_key
        AND A.product_key NOT LIKE '%Toothpaste%' AND B.product_key NOT LIKE '%Toothpaste%'
        GROUP BY item1, item2
        ORDER BY frequency DESC LIMIT 5
    """).fetchall()
    total_transactions = conn.execute("SELECT COUNT(DISTINCT transaction_id) FROM Fact_Sales").fetchone()[0] or 1
    market_basket = [{"pair": f"{r['item1']} + {r['item2']}", "percent": round(r['frequency'] / total_transactions * 100, 1)} for r in basket]

    return {
        "retention": retention,
        "clv_trend": clv_trend,
        "market_basket": market_basket
    }
@app.get("/analytics/overview-filtered")
def get_overview_filtered(period: str, city: str):
    conn = get_db()
    
    # 1. Handle Time Filter
    if period == "Last 30 Days":
        date_filter = "date_key >= date('now', '-30 days')"
        label_fmt = "date_key" # Show individual days
    else: # Last 6 Months
        date_filter = "date_key >= date('now', '-6 months')"
        label_fmt = "strftime('%Y-%m', date_key)" # Show months

    # 2. Handle City Filter
    city_clause = ""
    params = []
    if city != "All Cities":
        city_clause = "AND city = ?"
        params.append(city)

    # 3. Query for the Bar Chart
    query = f"""
        SELECT {label_fmt} as name, SUM(total_amount) as revenue 
        FROM Fact_Sales 
        WHERE {date_filter} {city_clause}
        GROUP BY name ORDER BY name ASC
    """
    chart_rows = conn.execute(query, params).fetchall()

    # 4. Query for Top Products
    prod_query = f"""
        SELECT product_key as name, COUNT(*) as sales 
        FROM Fact_Sales 
        WHERE {date_filter} {city_clause}
        GROUP BY name ORDER BY sales DESC LIMIT 5
    """
    prod_rows = conn.execute(prod_query, params).fetchall()

    return {
        "chartData": [{"name": r['name'], "revenue": r['revenue']} for r in chart_rows],
        "productList": [{"name": r['name'], "sales": r['sales']} for r in prod_rows]
    }
# --- 3. SMART FILTER ENDPOINT ---
# --- 3. SMART FILTER ENDPOINT ---
@app.get("/analytics/filter")
def get_filtered_data(period: str, city: str, year: str = "All Years"):
    conn = get_db()
    
    # --- 1. FIND THE SMART REFERENCE DATE ---
    # Find the latest date in the database based on the selected year
    base_date_query = "SELECT MAX(date_key) FROM Fact_Sales WHERE 1=1"
    base_params = []
    if year != "All Years":
        base_date_query += " AND strftime('%Y', date_key) = ?"
        base_params.append(year)
        
    max_date_row = conn.execute(base_date_query, base_params).fetchone()
    
    # If the database has data, use the max date as 'now'. Otherwise, use today.
    if max_date_row and max_date_row[0]:
        ref_date = max_date_row[0]
    else:
        ref_date = datetime.now().strftime("%Y-%m-%d")

    # --- 2. BUILD THE MAIN QUERY ---
    sql = "SELECT date_key, total_amount, product_key FROM Fact_Sales WHERE 1=1"
    params = []

    # Apply City Filter
    if city != "All Cities":
        sql += " AND city = ?"
        params.append(city)

    # Apply Year Filter
    if year != "All Years":
        sql += " AND strftime('%Y', date_key) = ?"
        params.append(year)

    # Apply Time Filter relative to the SMART reference date
    if period == "Last 30 Days":
        sql += f" AND date_key >= date('{ref_date}', '-30 days')"
        group_by = "date_key" # Group by day
    elif period == "Last 6 Months":
        sql += f" AND date_key >= date('{ref_date}', '-6 months')"
        group_by = "strftime('%Y-%m', date_key)" # Group by month
    elif period == "Last Year":
        sql += f" AND date_key >= date('{ref_date}', '-1 year')"
        group_by = "strftime('%Y-%m', date_key)" # Group by month
    else: # "All Time"
        if year != "All Years":
            group_by = "strftime('%Y-%m', date_key)" # Show months of that specific year
        else:
            group_by = "strftime('%Y', date_key)" # Show full years

    # --- 3. EXECUTE QUERIES ---
    rev_sql = f"SELECT {group_by} as label, SUM(total_amount) as val FROM ({sql}) GROUP BY label ORDER BY label"
    revenue_rows = conn.execute(rev_sql, params).fetchall()

    prod_sql = f"SELECT product_key, COUNT(*) as val FROM ({sql}) GROUP BY product_key ORDER BY val DESC LIMIT 5"
    prod_rows = conn.execute(prod_sql, params).fetchall()

    return {
        "revenue_chart": [{"name": r['label'] or 'Unknown', "revenue": r['val']} for r in revenue_rows],
        "top_products": [{"name": r['product_key'], "sales": r['val']} for r in prod_rows]
    }