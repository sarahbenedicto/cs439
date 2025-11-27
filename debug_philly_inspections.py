import pandas as pd

df = pd.read_json("data/raw/yelp_academic_dataset_business.json", lines=True, nrows=200000)

# Filter PA only
df_pa = df[df["state"] == "PA"]

print("\nTop 20 PA cities:")
print(df_pa["city"].value_counts().head(20))

print("\nSample PA rows:")
print(df_pa.head())
