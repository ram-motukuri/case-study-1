import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from task1 import cleaning_steps


def total_sales_per_store_last_month(sales, output_dir):
    today = pd.Timestamp.now().normalize()
    last_month_start = today - pd.DateOffset(months=1)
    last_month_start = last_month_start.replace(day=1)
    last_month_end = today - pd.DateOffset(days=today.day)
    sales['revenue'] = sales['quantity_sold'] * sales['price_sold']
    mask_last_month = (sales['sale_date'] >= last_month_start) & (sales['sale_date'] <= last_month_end)
    sales_last_month = sales.loc[mask_last_month].copy()
    # Total sales per store (last month)
    total_by_store = sales_last_month.groupby(['store_id'], as_index=False).agg(total_revenue=('revenue','sum')).sort_values('total_revenue', ascending=False)
    # round figures to 2 decimal places
    total_by_store['total_revenue'] = total_by_store['total_revenue'].round(2)
    sales_by_store_path = f"{output_dir}/sales_last_month_by_store.csv"
    total_by_store.to_csv(sales_by_store_path, index=False)
    logging.info(f'Sales by store (last month) saved to {sales_by_store_path}')


def top_five_products_sold_by_quantity(sales, output_dir):
    # Top 5 products sold by quantity
    top_products = sales.groupby(['product_id'], as_index=False).agg(total_quantity_sold=('quantity_sold','sum')).sort_values('total_quantity_sold', ascending=False).head(5)
    top_products_path = f"{output_dir}/top_5_products_by_quantity.csv"
    top_products.to_csv(top_products_path, index=False)
    logging.info(f'Top 5 products by quantity sold saved to {top_products_path}')


def low_stock_products(inventory, threshold, output_dir):
    # Products with low stock (threshold)
    low_stock = inventory[inventory['stock_quantity'] < threshold].copy()
    low_stock = low_stock.sort_values('stock_quantity')
    low_stock_path = f"{output_dir}/low_stock_products.csv"
    low_stock.to_csv(low_stock_path, index=False)
    logging.info(f'Low stock products (threshold={threshold}) saved to {low_stock_path}')

    # convert stock_quantity column to positive integers, set negatives to positive
    inventory['stock_quantity'] = pd.to_numeric(inventory['stock_quantity'], errors='coerce').fillna(0).astype(int)
    inventory.loc[inventory['stock_quantity'] < 0, 'stock_quantity'] = inventory['stock_quantity'].abs()

    low_stock = inventory[inventory['stock_quantity'] < threshold].copy()
    low_stock = low_stock.sort_values('stock_quantity', ascending=False)
    low_stock_path = f"{output_dir}/low_stock_products_stock_quantity_always_positive.csv"
    low_stock.to_csv(low_stock_path, index=False)
    logging.info(f'Low stock products (threshold={threshold}) saved to {low_stock_path}')


def gross_profit_per_product(sales, products, output_dir):
    sales['revenue'] = sales['quantity_sold'] * sales['price_sold']
    product_sales = sales.merge(products[['product_id','product_name','category','cost_price']], on='product_id', how='inner')
    # Gross profit per product
    product_sales['cost_total'] = product_sales['cost_price'] * product_sales['quantity_sold']
    product_sales['gross_profit'] = product_sales['revenue'] - product_sales['cost_total']
    profit_by_product = product_sales.groupby(['product_id','product_name','category'], as_index=False).agg(total_revenue=('revenue','sum'), total_cost=('cost_total','sum'), gross_profit=('gross_profit','sum')).sort_values('gross_profit', ascending=False)
    profit_path = f"{output_dir}/gross_profit_by_product.csv"
    profit_by_product.to_csv(profit_path, index=False)
    logging.info(f'Gross profit by product saved to {profit_path}')

sales = pd.read_csv("Preprocessed Datasets/sales_transaction_dataset.csv")
inventory = pd.read_csv("Preprocessed Datasets/inventory_dataset.csv")
stores = pd.read_csv("Preprocessed Datasets/store_dataset.csv")
products = pd.read_csv("Preprocessed Datasets/product_dataset.csv")

logging.info(f'Loaded files: sales={len(sales)}, inventory={len(inventory)}, stores={len(stores)}, products={len(products)}')

sales, inventory, stores, products = cleaning_steps(sales, inventory, stores, products)
output_dir = '../outputs/task3'
os.makedirs(output_dir, exist_ok=True)
total_sales_per_store_last_month(sales, output_dir)
top_five_products_sold_by_quantity(sales, output_dir)
low_stock_products(inventory, 10, output_dir)
gross_profit_per_product(sales, products, output_dir)
