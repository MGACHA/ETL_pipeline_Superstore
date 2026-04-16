# ETL_pipeline_Superstore
## Python ETL Pipeline
#### -Extracted raw CSV data
#### -Cleaned data (nulls, duplicates)
#### -Transformed columns (dates, naming)
#### -Loaded into SQLServer database
#### -Tools: Python, SQL
#### -Libraries: kaggle, Pandas, SQLAlchemy, pyodbc
#### -Dataset: sample-superstore.csv from kaggle
#### -Data quality checks (duplicates, date columns detection)

https://www.kaggle.com/datasets/vivek468/superstore-dataset-final


## Python ETL Pipeline 

1. Pandas SQLAlchemy is the integration of the Pandas data manipulation library with SQLAlchemy, Python's SQL toolkit and Object-Relational Mapper (ORM). It enables seamless reading of SQL database tables directly into Pandas DataFrames and writing DataFrames back to SQL databases, acting as a bridge between structured relational data and in-memory analysis.

``` bash ---
pip install pandas sqlalchemy pyodbc
```
2. Set up an ODBC connection, run db_connection.py
3. Create a database on SQL Server, run create_database.sql

4. Create ETL etl_pipeline.py with the below logic 



##### Import library pandas, read the sample-superstore.csv file from data folder with encoding.
#### EXTRACT
```python

#Extract (Read data)

import pandas as pd

# Extract data from CSV file
df = pd.read_csv("data/sample-superstore.csv", encoding="latin1")

# Show first rows
print(df.head())
```
##### Clean the data, standardise headers, remove duplicated rows, convert date column
#### TRANSFORM
```python

# Transform (Clean data)
# Remove null values
df = df.dropna()

# Remove duplicates
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

```
##### LOAD into SQL
```python

from sqlalchemy import create_engine

server = 'server_name'
database = 'etl_project'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

df.to_sql("clean_data", engine, if_exists="replace", index=False)

print("Data loaded successfully!")
```


## Python ETL_Pipeline_advanced.py
