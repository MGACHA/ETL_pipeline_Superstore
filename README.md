# ETL_pipeline_Superstore
Python ETL Pipeline sample-superstore.csv from kaggle


https://www.kaggle.com/datasets/vivek468/superstore-dataset-final



1. Pandas SQLAlchemy is the integration of the Pandas data manipulation library with SQLAlchemy, Python's SQL toolkit and Object-Relational Mapper (ORM). It enables seamless reading of SQL database tables directly into Pandas DataFrames and writing DataFrames back to SQL databases, acting as a bridge between structured relational data and in-memory analysis.

``` bash ---
pip install pandas sqlalchemy
```
2. Create etl_pipeline.py with the below logic 
Import library pandas, read the sample-superstore.csv file from data folder with encoding. Shows first 5 rows

``` python

import pandas as pd

# Extract data from CSV file
df = pd.read_csv("data/raw_data.csv", encoding="latin1")

# Show first rows
print(df.head())

```

```python
#Extract (Read data)

import pandas as pd

# Extract data from CSV file
df = pd.read_csv("data/sample-superstore.csv", encoding="latin1")

# Show first rows
print(df.head())



# Transform (Clean data)
# Remove null values
df = df.dropna()

# Remove duplicates
df = df.drop_duplicates()

# Standardize column names
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Example: convert date column
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

print(df.info())



from sqlalchemy import create_engine

server = 'magda'
database = 'etl_project'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

df.to_sql("clean_data", engine, if_exists="replace", index=False)

print("Data loaded successfully!")
```
