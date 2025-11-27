import pandas as pd
from pathlib import Path

#CONFIG 
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

RAW_INSPECTION_FILE = "clean_nyc_inspections.csv"
OUT_CLEAN_INSPECTIONS = PROCESSED_DIR / "nyc_inspections_clean.parquet"
OUT_RESTAURANTS_LATEST = PROCESSED_DIR / "nyc_restaurants_latest.parquet"

def load_raw_csv(path: Path) -> pd.DataFrame:
    """Load the NYC restaurant inspection CSV."""
    return pd.read_csv(path)


def clean_inspection_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and prepare NYC inspection data.

    Steps:
    1. Standardize column names
    2. Parse inspection dates
    3. Drop rows without dates
    4. Normalize grades
    5. Create a binary risk label (0 = A, 1 = non-A)
    """
    df = df.copy()

    # 1. Standardize column names
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # 2. Parse inspection date
    df["INSPECTION_DATE"] = pd.to_datetime(
        df["INSPECTION_DATE"],
        errors="coerce"
    )

    # 3. Drop rows without valid inspection dates
    df = df.dropna(subset=["INSPECTION_DATE"])

    # 4. Normalize GRADE and drop rows with missing GRADE
    df["GRADE"] = df["GRADE"].astype("string").str.strip()
    df = df[df["GRADE"].notna()].copy()

    # 5. Create HIGH_RISK: 0 for A, 1 for everything else
    df["HIGH_RISK"] = (df["GRADE"] != "A").astype("int64")

    return df



def build_latest_restaurant_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a table of UNIQUE restaurants with only their most recent inspection.

    Why?
    - Yelp businesses are unique
    - NYC inspection file has many inspections per restaurant (one per visit)
    """
    df_sorted = df.sort_values(["CAMIS", "INSPECTION_DATE"])

    # Keep the last inspection per CAMIS (restaurant id)
    latest = df_sorted.groupby("CAMIS").tail(1).copy()

    # Pick useful columns
    cols = [
        "CAMIS",
        "DBA",
        "BORO",
        "BUILDING",
        "STREET",
        "ZIPCODE",
        "CUISINE_DESCRIPTION",
        "INSPECTION_DATE",
        "SCORE",
        "GRADE",
        "HIGH_RISK",
        "Latitude",
        "Longitude"
    ]

    cols = [c for c in cols if c in latest.columns]
    latest = latest[cols]

    # Build a combined address for matching with Yelp
    latest["FULL_ADDRESS"] = (
        latest["BUILDING"].astype(str).str.strip()
        + " "
        + latest["STREET"].astype(str).str.strip()
        + ", NY "
        + latest["ZIPCODE"].astype(str)
    )

    return latest


def main():
    print(f"Loading NYC inspections from {RAW_INSPECTION_FILE} ...")
    df_raw = load_raw_csv(RAW_INSPECTION_FILE)
    print("Rows loaded:", len(df_raw))

    df_clean = clean_inspection_data(df_raw)
    print("Cleaned inspection rows:", len(df_clean))
    df_clean.to_parquet(OUT_CLEAN_INSPECTIONS, index=False)

    latest = build_latest_restaurant_table(df_clean)
    print("Unique restaurants:", latest["CAMIS"].nunique())
    latest.to_parquet(OUT_RESTAURANTS_LATEST, index=False)

    print("Done!")

if __name__ == "__main__":
    main()
