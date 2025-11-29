import pandas as pd

# 🔁 Update these paths if your files live somewhere else
INSPECTION_PATH = "clean_nyc_inspections.csv"
YELP_PATH       = "data/processed/yelp_nyc_api_businesses.csv"

print("Loading CSVs...\n")

ins = pd.read_csv(INSPECTION_PATH)
yelp = pd.read_csv(YELP_PATH)

print("=== NYC INSPECTIONS ===")
print("Shape:", ins.shape)
print("Columns:", list(ins.columns))
print("\nHead:")
print(ins.head(10))

print("\nBORO value counts:")
if "BORO" in ins.columns:
    print(ins["BORO"].value_counts(dropna=False))

# Optional: just Manhattan inspections
print("\nManhattan-only inspections (if BORO exists):")
if "BORO" in ins.columns:
    ins_manhattan = ins[ins["BORO"] == "Manhattan"]
    print("Shape (Manhattan):", ins_manhattan.shape)
    print(ins_manhattan.head(5))

print("\n=== YELP NYC API BUSINESSES ===")
print("Shape:", yelp.shape)
print("Columns:", list(yelp.columns))
print("\nHead:")
print(yelp.head(10))

# Quick check of key fields
for col in ["name", "address1", "city", "state", "postal_code"]:
    if col in yelp.columns:
        print(f"\nMissing values in '{col}':", yelp[col].isna().sum())

print("\nSample of name + zip pairs (Yelp):")
if {"name", "postal_code"}.issubset(yelp.columns):
    print(yelp[["name", "postal_code"]].head(10))

print("\nDone.")
