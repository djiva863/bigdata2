import pandas as pd
import json, os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

brands = ["Ferrari", "Lamborghini", "Porsche", "Aston Martin"]

for b in brands:
    with open(f"data/raw/cars_tech/{b}.json", "r") as f:
        df_tech = pd.DataFrame(json.load(f)["Results"])
    
    os.makedirs("data/formatted/cars_tech", exist_ok=True)
    df_tech[["Make_Name", "Model_Name"]].to_parquet(f"data/formatted/cars_tech/{b}.parquet")

    with open(f"data/raw/cars_news/{b}_news.json", "r") as f:
        df_news = pd.DataFrame(json.load(f)["articles"])
    
    os.makedirs("data/formatted/cars_news", exist_ok=True)
    df_news[["title", "publishedAt"]].to_parquet(f"data/formatted/cars_news/{b}.parquet")

