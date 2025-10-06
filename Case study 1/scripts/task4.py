import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from task1 import cleaning_steps
import sqlite3

def sales_by_region(output_dir, conn):
    query = """
    SELECT st.region, SUM(s.price_sold * s.quantity_sold) AS total_revenue
    FROM sales s
    JOIN stores st ON s.store_id = st.store_id
    GROUP BY st.region
    """
    sales_by_region = pd.read_sql_query(query, conn)
    sales_by_region_path = f"{output_dir}/sales_by_region_sql.csv"
    sales_by_region.to_csv(sales_by_region_path, index=False)

    logging.info(f'Sales by region saved to {sales_by_region_path}')

def top_three_stores_by_revenue(output_dir, conn):
    query = """
    SELECT st.store_id, st.store_name, SUM(s.price_sold * s.quantity_sold) AS total_revenue
    FROM sales s
    JOIN stores st ON s.store_id = st.store_id
    GROUP BY st.store_id, st.store_name
    ORDER BY total_revenue DESC
    LIMIT 3
    """
    top_stores = pd.read_sql_query(query, conn)
    top_stores_path = f"{output_dir}/top_3_stores_by_revenue_sql.csv"
    top_stores.to_csv(top_stores_path, index=False)

    logging.info(f'Top 3 stores by revenue saved to {top_stores_path}')

def unique_products_sold(output_dir, conn):
    query = """
    SELECT st.category, COUNT(DISTINCT s.product_id) AS unique_products_sold
    FROM sales s
    JOIN products st ON s.product_id = st.product_id
    GROUP BY st.category
    """
    unique_products_by_category = pd.read_sql_query(query, conn)
    unique_products_by_category_path = f"{output_dir}/unique_products_by_category_sql.csv"
    unique_products_by_category.to_csv(unique_products_by_category_path, index=False)

    logging.info(f'Unique products sold saved to {unique_products_by_category_path}')


sales = pd.read_csv("Preprocessed Datasets/sales_transaction_dataset.csv")
inventory = pd.read_csv("Preprocessed Datasets/inventory_dataset.csv")
stores = pd.read_csv("Preprocessed Datasets/store_dataset.csv")
products = pd.read_csv("Preprocessed Datasets/product_dataset.csv")

logging.info(f'Loaded files: sales={len(sales)}, inventory={len(inventory)}, stores={len(stores)}, products={len(products)}')

sales, inventory, stores, products = cleaning_steps(sales, inventory, stores, products)
output_dir = '../outputs/task4'
os.makedirs(output_dir, exist_ok=True)

conn = sqlite3.connect(':memory:')
sales.to_sql('sales', conn, index=False)
stores.to_sql('stores', conn, index=False)
products.to_sql('products', conn, index=False)

sales_by_region(output_dir, conn)
top_three_stores_by_revenue(output_dir, conn)
unique_products_sold(output_dir, conn)
conn.close()
