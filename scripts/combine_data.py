import pandas as pd
import os

p = "D:/BIG DATA/data/"
brands = ["Ferrari", "Lamborghini", "Porsche", "Aston Martin"]

t = pd.concat([pd.read_parquet(f"{p}formatted/cars_tech/{b}.parquet") for b in brands])

news_list = []
for b in brands:
    df = pd.read_parquet(f"{p}formatted/cars_news/{b}.parquet")
    df["brand"] = b
    news_list.append(df)
n = pd.concat(news_list)

res = pd.merge(t, n, left_on="Make_Name", right_on="brand")

os.makedirs(f"{p}usage", exist_ok=True)
res.to_parquet(f"{p}usage/final_luxury_cars.parquet")
print("OK")