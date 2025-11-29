import os
import pandas as pd
import re

#Paths (update if your layout is different)
INSPECTION_PATH = "clean_nyc_inspections.csv"
YELP_PATH       = "data/processed/yelp_nyc_api_businesses.csv"

OUT_CSV     = "data/processed/nyc_yelp_joined_v2.csv"
OUT_PARQUET = "data/processed/nyc_yelp_joined_v2.parquet"

os.makedirs("data/processed", exist_ok=True)

print("Loading data...")

# Read with ZIPs as strings to avoid leading-zero issues
ins = pd.read_csv(INSPECTION_PATH, dtype={"ZIPCODE": str}, low_memory=False)
yelp = pd.read_csv(YELP_PATH, dtype={"postal_code": str})

print("NYC inspections shape:", ins.shape)
print("Yelp businesses shape:", yelp.shape)

# Manhattan only
ins_m = ins[ins["BORO"] == "Manhattan"].copy()
print("Manhattan inspections shape:", ins_m.shape)

#Normalization helpers

def normalize_text(s):
    """Uppercase, strip, remove most punctuation and extra spaces."""
    if pd.isna(s):
        return ""
    s = str(s).upper()
    # Remove punctuation except spaces and digits/letters
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_zip(z):
    if pd.isna(z):
        return ""
    z = str(z).strip()
    # Keep only first 5 characters (ZIP+4 -> ZIP)
    return z[:5]

#Build cleaned keys for inspections

ins_m["name_clean"] = ins_m["DBA"].fillna("").map(normalize_text)
ins_m["zip_clean"] = ins_m["ZIPCODE"].map(normalize_zip)

# BUILDING + STREET -> address
ins_m["address_raw"] = (
    ins_m["BUILDING"].fillna("").astype(str).str.strip()
    + " "
    + ins_m["STREET"].fillna("").astype(str).str.strip()
)
ins_m["address_clean"] = ins_m["address_raw"].map(normalize_text)

#Build cleaned keys for Yelp

yelp["name_clean"] = yelp["name"].fillna("").map(normalize_text)
yelp["zip_clean"] = yelp["postal_code"].map(normalize_zip)

yelp["address_raw"] = yelp["address1"].fillna("").astype(str).str.strip()
yelp["address_clean"] = yelp["address_raw"].map(normalize_text)

#1) Exact match on (name_clean, zip_clean)

print("\nPerforming exact match on (name_clean, zip_clean)...")
exact = ins_m.merge(
    yelp,
    on=["name_clean", "zip_clean"],
    how="inner",
    suffixes=("_ins", "_yelp")
)
print("Exact matches found:", len(exact))

#2) Address match on (address_clean, zip_clean)

print("\nPerforming address match on (address_clean, zip_clean)...")
addr = ins_m.merge(
    yelp,
    left_on=["address_clean", "zip_clean"],
    right_on=["address_clean", "zip_clean"],
    how="inner",
    suffixes=("_ins", "_yelp")
)
print("Address matches found:", len(addr))

#Combine results and drop duplicates

combined = pd.concat([exact, addr], ignore_index=True)

# Keep one row per (CAMIS, yelp_id) pair
if "CAMIS" in combined.columns and "yelp_id" in combined.columns:
    before = len(combined)
    combined = combined.drop_duplicates(subset=["CAMIS", "yelp_id"])
    print("\nDropped duplicates on (CAMIS, yelp_id):", before - len(combined))

print("Total joined rows:", len(combined))

#Save outputs

combined.to_csv(OUT_CSV, index=False)
try:
    combined.to_parquet(OUT_PARQUET, index=False)
except Exception as e:
    print("Warning: could not write parquet:", e)

print("\nSaved joined dataset CSV at:", OUT_CSV)
print("Saved joined dataset Parquet at:", OUT_PARQUET)
print("Done.")
