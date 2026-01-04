import os
import time

print("--- DÉMARRAGE DE L'AUTOMATISATION (Simulation Airflow) ---")

def lancer_tache(nom, fichier):
    print(f"\n[EN COURS] Lancement de : {nom}...")
    start = time.time()
    retour = os.system(f'python "D:/BIG DATA/scripts/{fichier}"')
    
    if retour == 0:
        print(f"[OK] {nom} ")
    else:
        print(f"[ERREUR] Échec sur {nom}. Arrêt du pipeline.")
        exit()

lancer_tache("1. Ingestion (Tech & News)", "ingest_cars.py") 
lancer_tache("   -> Suite Ingestion", "ingest_news.py")
lancer_tache("2. Formatage Parquet", "format_data.py")
lancer_tache("3. Combinaison (Usage)", "combine_data.py")
lancer_tache("4. Indexation Kibana", "index_data.py")

print("\n--- PIPELINE TERMINÉ : DASHBOARD MIS À JOUR ---")
