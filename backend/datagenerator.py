import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'raw')
os.makedirs(DATA_DIR, exist_ok=True)

NUM_TRANSACTIONS = 5000  # Amount of new data to generate

# --- CONSTANTS (ALIGNED WITH KAGGLE DATASET) ---
CITIES = ['New York', 'Houston', 'Miami', 'Seattle', 'Atlanta', 'Boston', 'Dallas', 'Chicago', 'San Francisco', 'Los Angeles']
STORE_TYPES = ['Convenience Store', 'Supermarket', 'Warehouse Club', 'Pharmacy', 'Specialty Store', 'Department Store']
PAYMENT_METHODS = ['Debit Card', 'Mobile Payment', 'Cash', 'Credit Card']
CUSTOMER_CATEGORIES = ['Student', 'Professional', 'Young Adult', 'Senior Citizen', 'Teenager', 'Middle-Aged', 'Retiree', 'Homemaker']
SEASONS = ['Winter', 'Fall', 'Summer', 'Spring']
PROMOTIONS = [None, 'BOGO (Buy One Get One)', 'Discount on Selected Items']
PRODUCTS_LIST = [
    'Sponges', 'Cereal Bars', 'Banana', 'Potatoes', 'Onions', 'Hair Gel', 'Syrup', 'Milk', 'Carrots', 
    'Canned Soup', 'Shaving Cream', 'Pickles', 'Honey', 'Mop', 'Tuna', 'BBQ Sauce', 'Yogurt', 
    'Trash Cans', 'Bread', 'Toothpaste', 'Soap', 'Ketchup', 'Butter', 'Vinegar', 'Shower Gel', 
    'Orange', 'Peanut Butter', 'Hand Sanitizer', 'Ice Cream', 'Trash Bags', 'Tissues', 'Jam', 
    'Eggs', 'Chicken', 'Rice', 'Pasta', 'Soda', 'Water', 'Coffee', 'Tea', 'Sugar', 'Salt', 'Oil', 'Flour', 'Cheese', 'Beef'
]

def generate_aligned_data():
    print(f"🚀 Generating {NUM_TRANSACTIONS} rows of ALIGNED data (2023-2026)...")
    
    data = []
    # Kaggle data ends in 2022, so we start generating from 2023
    start_date = datetime(2023, 1, 1)
    
    # Kaggle IDs end around 1000030000, so we continue from there
    start_id = 1000030001 

    for i in range(NUM_TRANSACTIONS):
        # Generate date between Jan 2023 and Today (approx 3 years range)
        txn_date = start_date + timedelta(days=random.randint(0, 1100))
        
        num_items = random.randint(1, 10)
        selected_products = random.choices(PRODUCTS_LIST, k=num_items)
        
        # LOGIC: Ensure Season matches the Month (Realism Check)
        month = txn_date.month
        if month in [12, 1, 2]: season = 'Winter'
        elif month in [3, 4, 5]: season = 'Spring'
        elif month in [6, 7, 8]: season = 'Summer'
        else: season = 'Fall'

        row = {
            'Transaction_ID': start_id + i,
            'Date': txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            'Customer_Name': fake.name(),
            'Product': str(selected_products), # Matches Kaggle's string format "['A', 'B']"
            'Total_Items': num_items,
            'Total_Cost': round(random.uniform(5.00, 150.00), 2),
            'Payment_Method': random.choice(PAYMENT_METHODS),
            'City': random.choice(CITIES),
            'Store_Type': random.choice(STORE_TYPES),
            'Discount_Applied': random.choice([True, False]),
            'Customer_Category': random.choice(CUSTOMER_CATEGORIES),
            'Season': season,
            'Promotion': random.choice(PROMOTIONS)
        }
        data.append(row)

    df = pd.DataFrame(data)
    
    # Save to a NEW file so we don't overwrite Kaggle data
    output_path = os.path.join(DATA_DIR, 'Retail_Transactions_Dataset_Generated.csv')
    df.to_csv(output_path, index=False)
    print(f"✅ Generated '{output_path}'")

if __name__ == "__main__":
    generate_aligned_data()