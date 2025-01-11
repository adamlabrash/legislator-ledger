import pandas as pd

# Load the large CSV
input_file = "expenditures.csv"
chunk_size = 50000  # Adjust the number of rows per chunk

# Read and split into smaller files
for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
    chunk.to_csv(f"expenditures_part_{i+1}.csv", index=False)
    print(f"Saved expenditures_part_{i+1}.csv")
