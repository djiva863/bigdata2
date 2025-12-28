import requests, json, os

brands = ["Ferrari", "Lamborghini", "Porsche", "Aston Martin"]
path = "data/raw/cars_tech/"
os.makedirs(path, exist_ok=True)

for b in brands:
    url = f"https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/{b}?format=json"
    data = requests.get(url).json()
    
    with open(f"{path}{b}.json", "w") as f:
        json.dump(data, f)
    print(f"Importé : {b}")