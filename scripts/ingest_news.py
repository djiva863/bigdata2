import requests, json, os

api_key = "6dbe840e1e3249ce95ce9972c41e7674" 
brands = ["Ferrari", "Lamborghini", "Porsche", "Aston Martin"]
path = "data/raw/cars_news/"
os.makedirs(path, exist_ok=True)

for b in brands:
    url = f"https://newsapi.org/v2/everything?q={b}&apiKey={api_key}"
    data = requests.get(url).json()
    
    with open(f"{path}{b}_news.json", "w") as f:
        json.dump(data, f)
    print(f"Importé News : {b}")