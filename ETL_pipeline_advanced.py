import pandas as pd
import logging
from sqlalchemy import create_engine


# Logging configuration

# This part sends output to a file etl.log, NOT the terminal.

logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# This part sends output to a terminal.
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()   #this shows logs in terminal
    ]
)



# Extract

def extract(file_path):
    try:
        logging.info("Starting data extraction")

        df = pd.read_csv(file_path, encoding="latin1")

        logging.info(f"Data extracted successfully. Rows: {len(df)}")
        return df

    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise



# Transform

def transform(df):
    try:
        logging.info("Starting data transformation")

        # Clean column names
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        # Convert dates to format dd/mm/YYYY
        df['order_date'] = pd.to_datetime(df['order_date'], format='%m/%d/%Y')
        df['ship_date'] = pd.to_datetime(df['ship_date'], format='%m/%d/%Y')

        # Remove duplicates in full row
        df = df.drop_duplicates()

        # creating calculation 
        df['profit_margin'] = df['profit'] / df['sales']
        df['profit_margin'] = df['profit_margin'].round(2)

        logging.info("Transformation completed")
        return df

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise



# Load

def load(df, connection_string):
    try:
        logging.info("Starting data load")

        engine = create_engine(connection_string)

        df.to_sql("clean_data_v2", engine, if_exists="replace", index=False)

        logging.info("Data loaded successfully into SQL Server")

    except Exception as e:
        logging.error(f"Error during load: {e}")
        raise



# Main pipeline

def main():
    file_path = "data/sample-superstore.csv"

    connection_string = (
    "mssql+pyodbc://servername/etl_project"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

    )

    try:
        df = extract(file_path)
        df = transform(df)
        load(df, connection_string)

        logging.info("ETL pipeline completed successfully")

    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")



# Run script

if __name__ == "__main__":
    main()
    print("Pipeline started...")
