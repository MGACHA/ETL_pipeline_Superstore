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

# Create profit margin
df['profit_margin'] = df['profit'] / df['sales']
df['profit_margin'] = df['profit_margin'].round(2)

# Create delivery time 
df['delivery_days'] = (df['ship_date'] - df['order_date']).dt.days

# Extract date parts 
df['order_year'] = df['order_date'].dt.year
df['order_month'] = df['order_date'].dt.month
df['order_month_name'] = df['order_date'].dt.month_name()

# Sales category - segmentation
df['sales_category'] = pd.cut(
    df['sales'],
    bins=[0, 100, 500, 1000, 10000],
    labels=['Low', 'Medium', 'High', 'Very High']
)

print(df.info())



from sqlalchemy import create_engine

server = 'servername'
database = 'etl_project'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

df.to_sql("clean_data", engine, if_exists="replace", index=False)

print("Data loaded successfully!")
