from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

def monitor_engineers():
    """Мониторинг работы инженеров"""
    print("=" * 60)
    print("МОНИТОРИНГ РАБОТЫ ИНЖЕНЕРОВ")
    print("=" * 60)
    
    engineers = [
        {"id": 1, "name": "Алексеев А.А.", "specialization": "Смартфоны"},
        {"id": 2, "name": "Борисов Б.Б.", "specialization": "Ноутбуки"},
        {"id": 3, "name": "Васильев В.В.", "specialization": "ПК"},
        {"id": 4, "name": "Григорьев Г.Г.", "specialization": "Планшеты"},
        {"id": 5, "name": "Дмитриев Д.Д.", "specialization": "Универсал"},
    ]
    
    print("\n👨‍🔧 СТАТИСТИКА ИНЖЕНЕРОВ:")
    for engineer in engineers:
        completed = random.randint(5, 25)
        in_progress = random.randint(1, 5)
        avg_time = random.randint(2, 8)
        
        print(f"\n  {engineer['name']} ({engineer['specialization']})")
        print(f"    - Завершено заказов: {completed}")
        print(f"    - В работе: {in_progress}")
        print(f"    - Среднее время ремонта: {avg_time} часов")
    
    print("\n✓ Мониторинг завершен!")
    print("=" * 60)

def check_workload():
    """Проверка загруженности сервиса"""
    print("=" * 60)
    print("ПРОВЕРКА ЗАГРУЖЕННОСТИ СЕРВИСА")
    print("=" * 60)
    
    total_orders = random.randint(30, 60)
    active_orders = random.randint(15, 30)
    waiting_parts = random.randint(5, 15)
    
    print(f"\n📊 Всего заказов в системе: {total_orders}")
    print(f"⚙️  Активных заказов: {active_orders}")
    print(f"⏳ Ожидают запчасти: {waiting_parts}")
    print(f"📦 Готовы к выдаче: {total_orders - active_orders - waiting_parts}")
    
    workload_percent = (active_orders / total_orders) * 100
    print(f"\n📈 Загруженность: {workload_percent:.1f}%")
    
    if workload_percent > 70:
        print("⚠️  ВНИМАНИЕ: Высокая загруженность! Рекомендуется увеличить штат.")
    elif workload_percent < 30:
        print("✓ Загруженность в норме.")
    
    print("=" * 60)

def send_notifications():
    """Отправка уведомлений"""
    print("=" * 60)
    print("ОТПРАВКА УВЕДОМЛЕНИЙ")
    print("=" * 60)
    
    notifications = [
        "✉️  Отправлено SMS клиенту: Ваш iPhone готов к выдаче",
        "✉️  Отправлено email: Требуется подтверждение стоимости ремонта",
        "✉️  Push-уведомление инженеру: Новый срочный заказ",
    ]
    
    for notification in notifications:
        print(f"  {notification}")
    
    print("\n✓ Все уведомления отправлены!")
    print("=" * 60)

with DAG(
    dag_id='engineer_performance_monitor',
    default_args={
        'owner': 'repair_service',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Мониторинг работы инженеров и загруженности сервиса',
    schedule='0 9,14,18 * * *',  # В 9:00, 14:00 и 18:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['repair-service', 'monitoring', 'engineers'],
) as dag:
    
    task_monitor = PythonOperator(
        task_id='monitor_engineers',
        python_callable=monitor_engineers,
    )
    
    task_workload = PythonOperator(
        task_id='check_workload',
        python_callable=check_workload,
    )
    
    task_notify = PythonOperator(
        task_id='send_notifications',
        python_callable=send_notifications,
    )
    
    [task_monitor, task_workload] >> task_notify
