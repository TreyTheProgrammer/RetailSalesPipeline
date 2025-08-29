import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    # Connect to PostgreSQL
    try:
        engine = create_engine("postgresql://postgres:uR@*8vnxb5lqBk@db.sfqydiovsqouwlazstnr.supabase.co:5432/postgres")
    except:
        logging.error("Error occurred while connecting to the database.")
        raise   

    # Load CSVs
    try:
        df_stores = pd.read_csv("data/stores.csv")
        df_sales = pd.read_csv("data/sales.csv")
        df_products = pd.read_csv("data/products.csv")
    except:
        logging.error("Error occurred while reading CSV files.")
        raise

    # Write to database (raw schema)
    try:
        df_stores.to_sql("stores", engine, schema="raw", if_exists="replace", index=False)
        df_sales.to_sql("sales", engine, schema="raw", if_exists="replace", index=False)
        df_products.to_sql("products", engine, schema="raw", if_exists="replace", index=False)
    except:
        logging.error("Error occurred while ingesting data into the database.")
        raise

    logging.info("Data successfully ingested into the raw schema.")
    print("Data ingestion completed.")

