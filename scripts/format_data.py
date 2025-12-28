import pandas as pd
import json
import os

base_dir = os.getcwd()
raw_path = os.path.join(base_dir, "data", "raw")
formatted_path = os.path.join(base_dir, "data", "formatted")

brands = ["Ferrari", "Lamborghini", "Porsche", "Aston Martin"]

os.makedirs(os.path.join(formatted_path, "cars_tech"), exist_ok=True)
os.makedirs(os.path.join(formatted_path, "cars_news"), exist_ok=True)

for b in brands:
    print(f"--- Traitement de {b} ---")
    
    json_tech = os.path.join(raw_path, "cars_tech", f"{b}.json")
    if os.path.exists(json_tech):
        with open(json_tech, "r") as f:
            df_tech = pd.DataFrame(json.load(f)["Results"])
        df_tech = df_tech[["Make_Name", "Model_Name"]] # Normalisation 
        df_tech.to_parquet(os.path.join(formatted_path, "cars_tech", f"{b}.parquet"))
        print(f"OK : Parquet technique créé pour {b}")
    else:
        print(f"ERREUR : Fichier JSON introuvable ici : {json_tech}")

    json_news = os.path.join(raw_path, "cars_news", f"{b}_news.json") 
    if os.path.exists(json_news):
        with open(json_news, "r") as f:
            df_news = pd.DataFrame(json.load(f)["articles"])
        df_news = df_news[["title", "publishedAt"]]
        df_news["publishedAt"] = pd.to_datetime(df_news["publishedAt"]).dt.tz_localize(None) 
        df_news["brand"] = b
        df_news.to_parquet(os.path.join(formatted_path, "cars_news", f"{b}.parquet"))
        print(f"OK : Parquet news créé pour {b}")