import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from task1 import cleaning_steps


def merging_steps(sales, inventory, stores, products, cols_required, output_dir):
    # Merging steps
    merged = sales.merge(stores[['store_id','store_name','city','region']], on='store_id', how='left')
    merged = merged.merge(products[['product_id','product_name','category','cost_price']], on='product_id', how='left')
    merged = merged.merge(inventory[['store_id','product_id','stock_quantity']], on=['store_id','product_id'], how='left')
    merged = merged[cols_required]
    logging.info(f'Merged data shape: {merged.shape}')
    integrated_path = f"{output_dir}/integrated_dataset.csv"
    merged.to_csv(integrated_path, index=False)
    logging.info(f'Integrated dataset saved to {integrated_path}')

sales = pd.read_csv("Preprocessed Datasets/sales_transaction_dataset.csv")
inventory = pd.read_csv("Preprocessed Datasets/inventory_dataset.csv")
stores = pd.read_csv("Preprocessed Datasets/store_dataset.csv")
products = pd.read_csv("Preprocessed Datasets/product_dataset.csv")

logging.info(f'Loaded files: sales={len(sales)}, inventory={len(inventory)}, stores={len(stores)}, products={len(products)}')

sales, inventory, stores, products = cleaning_steps(sales, inventory, stores, products)
cols_required = ['transaction_id', 'sale_date', 'store_id', 'store_name', 'city', 'region', 'product_id', 'product_name', 'category', 'quantity_sold', 'price_sold', 'cost_price', 'stock_quantity']
output_dir = '../outputs/task2'
os.makedirs(output_dir, exist_ok=True)
merging_steps(sales, inventory, stores, products, cols_required, output_dir)