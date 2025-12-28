import os
import pandas as pd

# 1. Où est-ce que Python travaille ?
print(f"Emplacement actuel : {os.getcwd()}")

# 2. Est-ce qu'il voit tes fichiers JSON ?
tech_path = "data/raw/cars_tech/"
if os.path.exists(tech_path):
    print(f"Dossier JSON trouvé ! Fichiers : {os.listdir(tech_path)}")
else:
    print(f"ERREUR : Je ne vois pas le dossier {tech_path}")

# 3. Test de création d'un fichier Parquet
try:
    import pyarrow
    print("Bibliothèque Parquet : OK")
except:
    print("ERREUR : Il manque 'pyarrow'. Tape: pip install pyarrow")