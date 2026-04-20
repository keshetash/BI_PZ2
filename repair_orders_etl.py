from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def task_extract():
    print("=" * 50)
    print("ЗАДАЧА 1: Извлечение данных")
    print("Подключение к источнику данных...")
    print("Данные успешно извлечены!")
    print("=" * 50)
    return "Данные извлечены"

def task_transform():
    print("=" * 50)
    print("ЗАДАЧА 2: Обработка данных")
    print("Применение бизнес-логики...")
    print("Очистка и трансформация данных...")
    print("Данные успешно обработаны!")
    print("=" * 50)
    return "Данные обработаны"

def task_load():
    print("=" * 50)
    print("ЗАДАЧА 3: Загрузка данных")
    print("Подключение к хранилищу...")
    print("Сохранение результатов...")
    print("Данные успешно загружены!")
    print("=" * 50)
    return "Данные загружены"

with DAG(
    dag_id='repair_orders_etl',
    default_args={
        'owner': 'student',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Простой ETL pipeline для лабораторной работы №2',
    schedule='@daily',  
    start_date=datetime(2024, 1, 1),
    catchup=False,  
    tags=['lab2', 'etl', 'образование'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=task_extract,
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=task_transform,
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=task_load,
    )
    
    extract_task >> transform_task >> load_task
