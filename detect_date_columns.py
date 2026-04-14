import pandas as pd



# Extract

def extract(file_path):
    print("Loading data...")

    df = pd.read_csv(file_path, encoding="latin1")

    print(f"Data loaded. Columns: {len(df.columns)}")
    return df



# Detect date columns

def detect_date_columns(df):
    print("\nDetecting date columns...\n")

    date_columns = []

    for col in df.columns:

        # Step 1: Check column name
        if "date" in col.lower():

            try:
                # Step 2: Validate data
                converted = pd.to_datetime(df[col], errors='coerce')
                success_rate = converted.notna().mean()

                if success_rate > 0.8:
                    print(f"{col} confirmed as DATE column ({success_rate:.0%} valid)")
                    date_columns.append(col)
                else:
                    print(f"{col} has 'date' in name but data looks suspicious")

            except Exception:
                print(f"{col} failed conversion")

    return date_columns



# Main

def main():
    file_path = "data/sample-superstore.csv"

    df = extract(file_path)

    date_cols = detect_date_columns(df)

    print("\nDetected date columns:")
    print(date_cols)


# Run

if __name__ == "__main__":
    main()
