import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def safe_to_datetime(series):
    return pd.to_datetime(series, errors='coerce', infer_datetime_format=True)

def normalize_text(series):
    return series.fillna('Unknown').astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)

def cleaning_steps(sales, inventory, stores, products):
    # Cleaning steps
    sales_before = len(sales)
    sales = sales.drop_duplicates().reset_index(drop=True)
    logging.info(f'Removed {sales_before - len(sales)} duplicate sales rows')

    inventory_before = len(inventory)
    inventory = inventory.drop_duplicates().reset_index(drop=True)
    logging.info(f'Removed {inventory_before - len(inventory)} duplicate inventory rows')

    stores_before = len(stores)
    stores = stores.drop_duplicates().reset_index(drop=True)
    logging.info(f'Removed {stores_before - len(stores)} duplicate stores rows')

    products_before = len(products)
    products = products.drop_duplicates().reset_index(drop=True)
    logging.info(f'Removed {products_before - len(products)} duplicate products rows')

    # print column names of each dataframe
    logging.info(f'Sales columns: {sales.columns.tolist()}')
    logging.info(f'Inventory columns: {inventory.columns.tolist()}')
    logging.info(f'Stores columns: {stores.columns.tolist()}')
    logging.info(f'Products columns: {products.columns.tolist()}')

    # print data types of each dataframe
    logging.info(f'Sales dtypes:\n{sales.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Inventory dtypes:\n{inventory.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Stores dtypes:\n{stores.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Products dtypes:\n{products.dtypes}')

    sales['sale_date'] = safe_to_datetime(sales['sale_date'])
    inventory['last_updated'] = safe_to_datetime(inventory['last_updated'])
    logging.info(f"Sales 'sale_date' nulls after conversion: {sales['sale_date'].isnull().sum()}")
    logging.info(f"Inventory 'last_updated' nulls after conversion: {inventory['last_updated'].isnull().sum()}")


    stores['store_name'] = normalize_text(stores['store_name'])
    stores['city'] = normalize_text(stores['city'])
    stores['region'] = normalize_text(stores['region'])

    products['product_name'] = normalize_text(products['product_name'])
    products['category'] = normalize_text(products['category'])

    sales['quantity_sold'] = sales['quantity_sold'].fillna(0).astype(int)
    sales['price_sold'] = pd.to_numeric(sales['price_sold'], errors='coerce').fillna(0.0)

    inventory['stock_quantity'] = inventory['stock_quantity'].fillna(0).astype(int)

    # print data types of each dataframe
    logging.info(f'Sales dtypes:\n{sales.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Inventory dtypes:\n{inventory.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Stores dtypes:\n{stores.dtypes}')
    logging.info('-----------------------------------------')
    logging.info(f'Products dtypes:\n{products.dtypes}')

    return sales, inventory, stores, products

sales = pd.read_csv("Preprocessed Datasets/sales_transaction_dataset.csv")
inventory = pd.read_csv("Preprocessed Datasets/inventory_dataset.csv")
stores = pd.read_csv("Preprocessed Datasets/store_dataset.csv")
products = pd.read_csv("Preprocessed Datasets/product_dataset.csv")

logging.info(f'Loaded files: sales={len(sales)}, inventory={len(inventory)}, stores={len(stores)}, products={len(products)}')

sales, inventory, stores, products = cleaning_steps(sales, inventory, stores, products)