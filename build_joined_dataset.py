#Code not needed anymore

import pandas as pd
from pathlib import Path
import re

# PATHS 
INSPECTION_PATH = Path("data/processed/nyc_inspections_manhattan_clean.csv")
YELP_PARQUET_PATH = Path("data/processed/yelp_nyc_api_businesses.parquet")
OUT_CSV = Path("data/processed/nyc_yelp_joined.csv")
OUT_PARQUET = Path("data/processed/nyc_yelp_joined.parquet")


# CLEANING HELPERS

def clean_str(s):
    if pd.isna(s):
        return None
    s = str(s).upper().strip() # collapse multiple spaces, remove punctuation like commas & periods
    s = re.sub(r"[.,']", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def clean_zip(z):
    if pd.isna(z):
        return None
    z = str(z).strip()
    # keep only first 5 digits
    m = re.match(r"(\d{5})", z)
    return m.group(1) if m else None


def clean_address(addr):
    if pd.isna(addr):
        return None
    addr = str(addr).upper().strip()
    # normalize common street words
    addr = addr.replace(" AVENUE", " AVE")
    addr = addr.replace(" STREET", " ST")
    addr = addr.replace(" ROAD", " RD")
    addr = addr.replace(" BOULEVARD", " BLVD")
    addr = re.sub(r"[.,']", "", addr)
    addr = re.sub(r"\s+", " ", addr)
    return addr


# MAIN

print("Loading data...")

ins = pd.read_csv(INSPECTION_PATH)
yelp = pd.read_parquet(YELP_PARQUET_PATH)

print("NYC Inspection rows (Manhattan):", len(ins))
print("Yelp business rows:", len(yelp))

# Ensure ZIP is string
ins["ZIPCODE"] = ins["ZIPCODE"].astype(str)
yelp["postal_code"] = yelp["postal_code"].astype(str)

# Clean name + zip for exact name matching
ins["name_clean"] = ins["DBA"].apply(clean_str)
ins["zip_clean"] = ins["ZIPCODE"].apply(clean_zip)

yelp["name_clean"] = yelp["name"].apply(clean_str)
yelp["zip_clean"] = yelp["postal_code"].apply(clean_zip)

# Clean addresses for address-based matching
ins["address_clean"] = (
    ins["BUILDING"].astype(str).str.upper().str.strip()
    + " "
    + ins["STREET"].astype(str).str.upper().str.strip()
)
ins["address_clean"] = ins["address_clean"].apply(clean_address)

yelp["address_clean"] = yelp["address1"].apply(clean_address)

# 1) EXACT MATCH: (name, zip)

print("Performing exact match on (name_clean, zip_clean)...")

exact_join = pd.merge(
    ins,
    yelp,
    on=["name_clean", "zip_clean"],
    how="inner",
    suffixes=("_ins", "_yelp"),
)

print("Exact matches found:", len(exact_join))

# 2) ADDRESS MATCH: (address, zip)

print("Performing address match on (address_clean, zip_clean)...")

addr_join = pd.merge(
    ins,
    yelp,
    on=["address_clean", "zip_clean"],
    how="inner",
    suffixes=("_ins", "_yelp"),
)

print("Address matches found:", len(addr_join))

# COMBINE & DEDUP

print("Combining match results...")

combined = pd.concat([exact_join, addr_join], ignore_index=True)

# Deduplicate by NYC CAMIS (restaurant ID) + yelp_id
if "CAMIS" in combined.columns and "yelp_id" in combined.columns:
    combined = combined.drop_duplicates(subset=["CAMIS", "yelp_id"])
else:
    combined = combined.drop_duplicates()

print("Total unique matches:", len(combined))

# SAVE OUTPUT

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

combined.to_csv(OUT_CSV, index=False)
combined.to_parquet(OUT_PARQUET, index=False)

print("Saved joined dataset:")
print(" - CSV    :", OUT_CSV)
print(" - Parquet:", OUT_PARQUET)
