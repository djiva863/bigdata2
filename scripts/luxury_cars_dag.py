from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Configuration : Le DAG se lance tous les jours
default_args = {
    'owner': 'etudiant',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG('luxury_cars_pipeline', 
         default_args=default_args, 
         schedule_interval='@daily', 
         catchup=False) as dag:

    # 1. Ingestion (On lance les deux scripts en même temps)
    t1_tech = BashOperator(
        task_id='ingest_tech',
        bash_command='python "D:/BIG DATA/scripts/ingest_cars.py"'
    )
    
    t1_news = BashOperator(
        task_id='ingest_news',
        bash_command='python "D:/BIG DATA/scripts/ingest_news.py"'
    )

    # 2. Formatage (Attend que l'ingestion soit finie)
    t2 = BashOperator(
        task_id='format_data',
        bash_command='python "D:/BIG DATA/scripts/format_data.py"'
    )

    # 3. Combinaison (Attend que le formatage soit fini)
    t3 = BashOperator(
        task_id='combine_data',
        bash_command='python "D:/BIG DATA/scripts/combine_data.py"'
    )

    # 4. Indexation (Envoie tout vers Kibana à la fin)
    t4 = BashOperator(
        task_id='index_data',
        bash_command='python "D:/BIG DATA/scripts/index_data.py"'
    )

    # L'ordre exact du pipeline
    [t1_tech, t1_news] >> t2 >> t3 >> t4