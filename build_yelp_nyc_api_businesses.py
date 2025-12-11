#Code is meant to using the YELP API Key, be able to make an NYC restuarant database. 

import os
import time
import requests
import pandas as pd
from itertools import product
from pathlib import Path

# CONFIG

#API Key needed. I deleted it over here
#API_KEY = os.environ.get("YELP_API_KEY") you have to make your own
if not API_KEY:
    raise RuntimeError("YELP_API_KEY environment variable is not set.")

HEADERS = {"Authorization": f"Bearer {API_KEY}"}
URL = "https://api.yelp.com/v3/businesses/search"

# Manhattan sub-areas (rough centers) to diversify results
MANHATTAN_AREAS = [
    {"name": "midtown", "latitude": 40.7580, "longitude": -73.9855},
    {"name": "upper_west", "latitude": 40.7870, "longitude": -73.9754},
    {"name": "upper_east", "latitude": 40.7736, "longitude": -73.9566},
    {"name": "lower_manhattan", "latitude": 40.7081, "longitude": -74.0064},
]
# Yelp radius (meters). 8000 ~ enough to cover neighborhood chunks
RADIUS = 8000
# Different "terms" and a plain restaurant query to diversify results
SEARCH_TERMS = [
    None,# no term, just restaurants
    "lunch", "dinner", "breakfast", "brunch", "pizza", "sushi", "chinese", "italian","mexican"
]

LIMIT = 50      # max Yelp page size
MAX_OFFSET = 200  # offsets: 0,50,100,150. Stop when 200 reached or error.
# HELPERS
def fetch_page(params, offset):
    """Call Yelp API for a single page with given params + offset."""
    full_params = params.copy()
    full_params["offset"] = offset
    full_params["limit"] = LIMIT
    full_params["categories"] = "restaurants"
    full_params["sort_by"] = "best_match"
    r = requests.get(URL, headers=HEADERS, params=full_params)
    print(f"Request area={params.get('area_name')} term={params.get('term')} "
          f"offset={offset}, status={r.status_code}")
    if r.status_code == 429:
        # Rate limited: back off and try again
        print("Hit rate limit (429). Sleeping for 5 seconds...")
        time.sleep(5)
        r = requests.get(URL, headers=HEADERS, params=full_params)
        print(f"Retry status={r.status_code}")

    r.raise_for_status()
    return r.json()

def build_param_grid():
    """Create list of param dicts combining area and term."""
    param_list = []
    for area, term in product(MANHATTAN_AREAS, SEARCH_TERMS):
        params = {
            "latitude": area["latitude"],
            "longitude": area["longitude"],
            "radius": RADIUS,
            "term": term,
            "area_name": area["name"],  # custom field, not sent to API
        }
        if term is not None:
            params["term"] = term
        else:
            # If term None, remove it from params (just restaurants)
            params.pop("term", None)
        param_list.append(params)
    return param_list
# MAIN SCRAPE
def main():
    all_businesses = []
    param_grid = build_param_grid()

    for params in param_grid:
        # We only send recognized API params; area_name is just for logging
        api_params = {k: v for k, v in params.items()
                      if k in ["latitude", "longitude", "radius", "term"]}
        offset = 0
        while offset < MAX_OFFSET:
            try:
                data = fetch_page({**api_params, "area_name": params["area_name"]}, offset)
            except requests.HTTPError as e:
                # Most likely 400 if offset > total; break this query loop
                print(f"HTTPError for area={params['area_name']} "
                      f"term={params.get('term')} offset={offset}: {e}")
                break
            businesses = data.get("businesses", [])
            if not businesses:
                break
            all_businesses.extend(businesses)
            # Yelp returns 'total' for the query; stop if we passed it
            total = data.get("total", 0)
            if offset + LIMIT >= total:
                break
            offset += LIMIT
            time.sleep(0.2)  # be nice to the API
    print(f"Total raw businesses fetched (with duplicates): {len(all_businesses)}")
    # DEDUP & STRUCTURE
    rows = []
    seen_ids = set()

    for b in all_businesses:
        bid = b.get("id")
        if not bid or bid in seen_ids:
            continue
        seen_ids.add(bid)
        loc = b.get("location", {}) or {}
        cats = [c.get("title") for c in b.get("categories", []) if c.get("title")]
        coords = b.get("coordinates", {}) or {}
        rows.append({
            "yelp_id": bid,
            "name": b.get("name"),
            "rating": b.get("rating"),
            "review_count": b.get("review_count"),
            "price": b.get("price"),
            "categories": ", ".join(cats),
            "address1": loc.get("address1"),
            "city": loc.get("city"),
            "state": loc.get("state"),
            "postal_code": loc.get("zip_code"),
            "latitude": coords.get("latitude"),
            "longitude": coords.get("longitude"),
            "url": b.get("url"),
        })
    df = pd.DataFrame(rows)
    print(f"Unique Yelp businesses after dedup: {len(df)}")
    # SAVE (PARQUET + CSV)
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / "yelp_nyc_api_businesses.parquet"
    csv_path = out_dir / "yelp_nyc_api_businesses.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    print("Saved Yelp API businesses:")
    print("Parquet:", parquet_path)
    print("CSV:", csv_path)


if __name__ == "__main__":
    main()
