import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

BUSINESS_PATH = RAW_DIR / "yelp_academic_dataset_business.json"
OUT_FILE = PROCESSED_DIR / "yelp_philly_businesses.parquet"

def load_philly_businesses(path: Path) -> pd.DataFrame:
    chunks = pd.read_json(path, lines=True, chunksize=100_000)

    kept = []
    total = 0

    for i, chunk in enumerate(chunks, start=1):

        # Filter Pennsylvania only
        chunk = chunk[chunk["state"] == "PA"]

        # Filter Philadelphia specifically
        chunk = chunk[chunk["city"] == "Philadelphia"]

        # Filter categories for restaurants
        chunk = chunk[chunk["categories"].notna()]
        mask_food = (
            chunk["categories"].str.contains("Restaurant", case=False, na=False)
            | chunk["categories"].str.contains("Food", case=False, na=False)
        )
        chunk = chunk[mask_food]

        if not chunk.empty:
            kept.append(chunk)
            total += len(chunk)
            print(f"Kept so far: {total:,}", end="\r")

    print()
    if not kept:
        print("No Philly Yelp restaurants found.")
        return pd.DataFrame()

    df = pd.concat(kept, ignore_index=True)

    print(f"Total Philly Yelp restaurants: {len(df):,}")

    # Keep relevant columns
    cols_keep = [
        "business_id",
        "name",
        "address",
        "city",
        "state",
        "postal_code",
        "latitude",
        "longitude",
        "stars",
        "review_count",
        "categories",
        "is_open",
    ]
    df = df[cols_keep]

    # Create full address string
    df["FULL_ADDRESS"] = (
        df["address"].astype(str).str.strip()
        + ", Philadelphia, PA "
        + df["postal_code"].astype(str).str.strip()
    )

    return df


def main():
    print(f"Loading Yelp businesses from {BUSINESS_PATH} ...")

    df = load_philly_businesses(BUSINESS_PATH)
    if df.empty:
        return

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_FILE, index=False)

    print(f"Saved Philadelphia Yelp restaurants to {OUT_FILE}")


if __name__ == "__main__":
    main()
