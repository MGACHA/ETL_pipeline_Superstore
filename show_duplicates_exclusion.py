import pandas as pd



# Extract

def extract(file_path):
    print(" Extracting data...")

    df = pd.read_csv(file_path, encoding="latin1")

    print(f"Data loaded. Rows: {len(df)}")
    return df



# Show duplicates

def show_duplicates(df):
    print("\nChecking for duplicates (excluding row_id)...\n")

    # Exclude row_id column
    columns_to_check = [col for col in df.columns if col != 'Row ID']

    # Find duplicates based on remaining columns
    duplicates = df[df.duplicated(subset=columns_to_check, keep=False)]

    print(f"Number of duplicate rows (excluding row_id): {len(duplicates)}")

    if not duplicates.empty:
        print("\nSample duplicates:")
        print(duplicates.head(10))
    else:
        print(" No duplicates found.")

    return duplicates



# Main

def main():
    file_path = "data/sample-superstore.csv"

    df = extract(file_path)

    duplicates = show_duplicates(df)

    print("\nScript finished.")



# Run script

if __name__ == "__main__":
    main()
