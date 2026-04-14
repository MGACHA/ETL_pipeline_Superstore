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

server = 'servername'
database = 'etl_project'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

df.to_sql("clean_data", engine, if_exists="replace", index=False)

print("Data loaded successfully!")
