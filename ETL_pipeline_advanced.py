
#ETL Pipeline: Kaggle - Pandas - SQL Server

#  1. Extract   – authenticate, download dataset from Kaggle and read into DataFrame
#  2. Transform – clean and standardise the data
#  3. Load      – push the result to SQL Server via SQLAlchemy


import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import kaggle


# CONFIGURATION DETAILS  

KAGGLE_DATASET  = "vivek468/superstore-dataset-final"
DOWNLOAD_PATH   = "data" # folder where the CSV will be saved in repo
CSV_FILE        = os.path.join(DOWNLOAD_PATH, "Sample - Superstore.csv") #name of the file form kaggle
CSV_ENCODING    = "latin1"

SQL_SERVER      = "servername"
SQL_DATABASE    = "etl_project"
SQL_TABLE       = "Superstore_clean_data"
ODBC_DRIVER     = "ODBC+Driver+17+for+SQL+Server"

# STEP 1 – EXTRACT

def extract() -> pd.DataFrame:
    """Authenticate with Kaggle API KEY saved in C:\Users\username\.kaggle, download the dataset and read it into a DataFrame."""
    print("\n[1/3] Extracting data from Kaggle...")

    # Authenticate & download
    kaggle.api.authenticate()

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    kaggle.api.dataset_download_files(
        KAGGLE_DATASET,
        path=DOWNLOAD_PATH,
        unzip=True
    )

    # Optional: save dataset metadata alongside the data
    kaggle.api.dataset_metadata(KAGGLE_DATASET, path=DOWNLOAD_PATH)

    print(f" Dataset downloaded and extracted to {DOWNLOAD_PATH}/'")

    # Read into DataFrame
    if not os.path.exists(CSV_FILE):
        print(f" File not found: {CSV_FILE}")
        sys.exit(1)

    df = pd.read_csv(CSV_FILE, encoding=CSV_ENCODING)

    print(f" Loaded {len(df):,} rows × {len(df.columns)} columns")
    print(df.head())

    return df

# STEP 2 – TRANSFORM - data checking

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardise the DataFrame."""
    print("\n[2/3] Transforming data...")

    # Remove null values
    before = len(df)
    df = df.dropna()
    print(f" Dropped nulls: {before - len(df):,} rows removed")

    # Remove duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    print(f" Dropped duplicates: {before - len(df):,} rows removed")

    # Standardise column names (lowercase + underscores)
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Parse any date columns to datetime dd/mm/yyyy
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        print(f" Parsing dates: {col}")
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    print(f"\n Transform complete – {len(df):,} rows remaining")
    print(df.info())

    return df


# STEP 3 – LOAD

def load(df: pd.DataFrame) -> None:
    """Write the cleaned DataFrame to SQL Server."""
    print("\n[3/3] Loading data into SQL Server...")

    connection_string = (
        f"mssql+pyodbc://@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={ODBC_DRIVER}"
    )

    engine = create_engine(connection_string)

    df.to_sql(SQL_TABLE, engine, if_exists="replace", index=False)

    print(f" Data loaded successfully into [{SQL_DATABASE}].[dbo].[{SQL_TABLE}]")


# MAIN

def main() -> None:
    print("=" * 50)
    print("ETL PIPELINE  –  Superstore Dataset")
    print("=" * 50)

    df = extract()
    df = transform(df)
    load(df)

    print("\n Pipeline finished successfully.")


if __name__ == "__main__":
    main()
