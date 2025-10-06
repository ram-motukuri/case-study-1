# Case Study 1: Retail Data ETL Pipeline

This case study demonstrates a comprehensive ETL (Extract-Transform-Load) pipeline for processing retail transactional data to generate integrated datasets and analytical insights.

## 📁 Project Structure

```
Case study 1/
├── Case Study Document.pdf          # Project requirements and specifications
├── documentation.txt               # Detailed ETL pipeline documentation
├── README.md                      # This file
├── Preprocessed Datasets/         # Clean input datasets
│   ├── inventory_dataset.csv      # Cleaned inventory data
│   ├── product_dataset.csv        # Cleaned product information
│   ├── sales_transaction_dataset.csv  # Cleaned sales transactions
│   └── store_dataset.csv          # Cleaned store information
├── scripts/                       # Python ETL scripts
│   ├── task1.py                  # Data cleaning and preprocessing
│   ├── task2.py                  # Data integration and merging
│   ├── task3.py                  # Business analytics using Pandas
│   └── task4.py                  # SQL-based analytics
└── outputs/                      # Generated outputs and results
    ├── task2/                    # Integration outputs
    │   └── integrated_dataset.csv
    ├── task3/                    # Pandas analytics outputs
    │   ├── gross_profit_by_product.csv
    │   ├── low_stock_products.csv
    │   ├── low_stock_products_stock_quantity_always_positive.csv
    │   ├── sales_last_month_by_store.csv
    │   └── top_5_products_by_quantity.csv
    ├── task4/                    # SQL analytics outputs
    │   ├── sales_by_region_sql.csv
    │   ├── top_3_stores_by_revenue_sql.csv
    │   └── unique_products_by_category_sql.csv
    └── task5/                    # ETL workflow diagrams
        ├── ETL Workflow Diagram.png
        └── Realtime extraction Workflow Diagram.png
```

## 🎯 Project Objectives

The ETL pipeline processes retail transactional data to generate:
- Sales analysis by store and region
- Top-selling products identification
- Low-stock product alerts
- Gross profit calculations per product
- Comprehensive business intelligence reports

## 📊 Data Sources

### Input Datasets
Located in [`Preprocessed Datasets/`](Preprocessed Datasets/):

1. **`sales_transaction_dataset.csv`** - Sales transaction records
2. **`inventory_dataset.csv`** - Product inventory levels
3. **`product_dataset.csv`** - Product information and pricing
4. **`store_dataset.csv`** - Store location and details

## 🔧 ETL Pipeline Components

### Task 1: Data Cleaning ([`task1.py`](scripts/task1.py))
**Functions:**
- `safe_to_datetime()` - Handles datetime conversion with error handling
- `normalize_text()` - Standardizes text fields
- `cleaning_steps()` - Main data cleaning orchestration

**Operations:**
- Duplicate removal
- Missing value handling
- Data type standardization
- Text normalization

### Task 2: Data Integration ([`task2.py`](scripts/task2.py))
**Functions:**
- `merging_steps()` - Merges all datasets into integrated format

**Output:** [`outputs/task2/integrated_dataset.csv`](outputs/task2/integrated_dataset.csv)

### Task 3: Pandas Analytics ([`task3.py`](scripts/task3.py))
**Functions:**
- `total_sales_per_store_last_month()` - Sales analysis by store
- `top_five_products_sold_by_quantity()` - Top products identification
- `low_stock_products()` - Inventory alerts
- `gross_profit_per_product()` - Profitability analysis

**Outputs in [`outputs/task3/`](outputs/task3/):**
- `sales_last_month_by_store.csv` - Monthly sales by store
- `top_5_products_by_quantity.csv` - Top selling products
- `low_stock_products.csv` - Products below stock threshold
- `low_stock_products_stock_quantity_always_positive.csv` - Positive stock validation
- `gross_profit_by_product.csv` - Product profitability

### Task 4: SQL Analytics ([`task4.py`](scripts/task4.py))
**Functions:**
- `sales_by_region()` - Regional sales analysis
- `top_three_stores_by_revenue()` - Revenue-based store ranking
- `unique_products_sold()` - Product variety analysis by category

**Outputs in [`outputs/task4/`](outputs/task4/):**
- `sales_by_region_sql.csv` - Sales aggregated by region
- `top_3_stores_by_revenue_sql.csv` - Highest revenue stores
- `unique_products_by_category_sql.csv` - Product diversity metrics

### Task 5: Workflow Documentation
**Outputs in [`outputs/task5/`](outputs/task5/):**
- `ETL Workflow Diagram.png` - Visual ETL process flow
- `Realtime extraction Workflow Diagram.png` - Real-time processing architecture

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- pandas
- numpy
- sqlite3 (for SQL operations)

### Running the Pipeline - Go inside scripts folder and run it

1. **Data Cleaning:**
   ```python
   python task1.py
   ```

2. **Data Integration:**
   ```python
   python task2.py
   ```

3. **Analytics (Pandas):**
   ```python
   python task3.py
   ```

4. **Analytics (SQL):**
   ```python
   python task4.py
   ```

## 📈 Key Business Insights

The pipeline generates several critical business metrics:

1. **Sales Performance** - Monthly sales trends by store location
2. **Inventory Management** - Low stock alerts and reorder recommendations
3. **Product Analytics** - Top performing products by quantity sold
4. **Profitability Analysis** - Gross profit margins by product
5. **Regional Performance** - Sales distribution across geographic regions
6. **Store Efficiency** - Revenue rankings and performance metrics

## 📋 Output Files Description

### Integration Outputs
- **`integrated_dataset.csv`** - Master dataset combining all sources

### Analytics Outputs
- **Sales Analysis**: Store-wise and region-wise sales breakdowns
- **Product Analysis**: Top products, profit margins, and category insights
- **Inventory Analysis**: Stock levels and reorder alerts
- **Performance Metrics**: Store rankings and regional comparisons

## 📚 Documentation

- **[`Case Study Document.pdf`](Case Study Document.pdf)** - Complete project requirements
- **[`documentation.txt`](documentation.txt)** - Detailed technical documentation
- **Workflow Diagrams** - Visual representation of ETL processes

## 🔄 Pipeline Features

- **Data Quality Assurance** - Comprehensive cleaning and validation
- **Scalability** - Designed for both batch and real-time processing
- **Modularity** - Independent task components for flexible execution
- **Error Handling** - Robust error management and logging
- **Performance Optimization** - Efficient data processing and storage

---

*This ETL pipeline ensures data consistency, reliability, and scalability while providing actionable business insights for retail operations.*
