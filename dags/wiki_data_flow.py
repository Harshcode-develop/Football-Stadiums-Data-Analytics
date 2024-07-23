from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
from pipelines.wiki_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#--------------------------------DAG---------------------------------------------
dag=DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner":"Harsh Bhosale",
        "start_date": datetime(year=2024, month=7, day=21),
    },
    schedule_interval=None,
    catchup=False
)


#extraction
extract_data_from_wiki=PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

#preprocessing
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    provide_context=True,
    python_callable=write_wikipedia_data,
    dag=dag

)
extract_data_from_wiki >> transform_wikipedia_data >> write_wikipedia_data

