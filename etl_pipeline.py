#Extract (Read data)

import pandas as pd

# Extract data from CSV file
df = pd.read_csv("data/sample-superstore.csv", encoding="latin1")

# Show first rows
print(df.head())



# Transform (Clean data)
# Remove null values
df = df.dropna()

# Remove duplicates in rows
df = df.drop_duplicates()

# Standardize column names
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Convert date column to DD/MM/YYYY format
for col in df.columns:
    if 'date' in col.lower():
        print(f"Converting {col} to datetime")
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')

print(df.info())



from sqlalchemy import create_engine

server = 'servername'
database = 'etl_project'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

df.to_sql("clean_data", engine, if_exists="replace", index=False)

print("Data loaded successfully!")
