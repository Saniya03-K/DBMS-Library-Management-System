import pandas as pd

# List of CSV files to create subsets from
files = ["books.csv", "borrowers.csv", "transactions.csv"]

# Define the percentages for subsets
fractions = {
    "25": 0.25,
    "50": 0.50,
    "75": 0.75,
    "100": 1.0
}

for file in files:
    df = pd.read_csv(file)
    for label, frac in fractions.items():
        subset = df.sample(frac=frac, random_state=42) if frac < 1.0 else df
        out_file = file.replace(".csv", f"_{label}.csv")
        subset.to_csv(out_file, index=False)
        print(f"Created {out_file} with {len(subset)} records.")
