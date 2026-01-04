import pandas as pd
from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "6Bb*VGO3ww-NBvQaTJFi"),
    verify_certs=False
)

df = pd.read_parquet("D:/BIG DATA/data/usage/final_luxury_cars.parquet")

for i, row in df.iterrows():
    es.index(index="luxury_cars", document=row.to_dict())

print("Données envoyées ! Tu peux maintenant créer ton Dashboard.")