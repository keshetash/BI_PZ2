from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import random
from faker import Faker

fake = Faker('ru_RU')

def generate_clients_data(**context):
    """Генерация данных о клиентах"""
    print("=" * 60)
    print("Генерация данных о клиентах сервиса...")
    print("=" * 60)
    
    clients = []
    for i in range(1, 21):
        client = {
            'client_id': i,
            'name': fake.name(),
            'phone': fake.phone_number(),
            'email': fake.email(),
            'address': fake.address(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today')
        }
        clients.append(client)
    
    df_clients = pd.DataFrame(clients)
    
    print(f"Сгенерировано клиентов: {len(df_clients)}")
    print("\nПервые 5 клиентов:")
    print(df_clients.head())
    
    context['ti'].xcom_push(key='clients_data', value=df_clients.to_json())
    print("\n✓ Данные о клиентах сохранены!")
    print("=" * 60)

def generate_devices_data(**context):
    """Генерация данных об устройствах"""
    print("=" * 60)
    print("Генерация данных об устройствах...")
    print("=" * 60)
    
    device_types = ['Смартфон', 'Ноутбук', 'Планшет', 'ПК', 'Телевизор']
    brands = {
        'Смартфон': ['iPhone', 'Samsung', 'Xiaomi', 'Huawei'],
        'Ноутбук': ['Lenovo', 'HP', 'Dell', 'ASUS'],
        'Планшет': ['iPad', 'Samsung Tab', 'Huawei MatePad'],
        'ПК': ['Custom Build', 'HP', 'Dell'],
        'Телевизор': ['Samsung', 'LG', 'Sony', 'Philips']
    }
    
    devices = []
    for i in range(1, 31):
        device_type = random.choice(device_types)
        brand = random.choice(brands[device_type])
        device = {
            'device_id': i,
            'type': device_type,
            'brand': brand,
            'model': f"{brand} {random.randint(1, 15)}",
            'year': random.randint(2018, 2024)
        }
        devices.append(device)
    
    df_devices = pd.DataFrame(devices)
    
    print(f"Сгенерировано устройств: {len(df_devices)}")
    print("\nРаспределение по типам:")
    print(df_devices['type'].value_counts())
    
    context['ti'].xcom_push(key='devices_data', value=df_devices.to_json())
    print("\n✓ Данные об устройствах сохранены!")
    print("=" * 60)

def generate_repair_orders(**context):
    """Генерация данных о заказах на ремонт"""
    print("=" * 60)
    print("Генерация данных о заказах на ремонт...")
    print("=" * 60)
    
    clients_json = context['ti'].xcom_pull(key='clients_data', task_ids='generate_clients')
    devices_json = context['ti'].xcom_pull(key='devices_data', task_ids='generate_devices')
    
    df_clients = pd.read_json(clients_json)
    df_devices = pd.read_json(devices_json)
    
    issue_types = [
        'Разбит экран',
        'Не включается',
        'Проблемы с батареей',
        'Не заряжается',
        'Программный сбой',
        'Перегрев',
        'Не работает камера',
        'Проблемы со звуком',
        'Залитие жидкостью',
        'Механическое повреждение'
    ]
    
    statuses = ['Принят', 'В работе', 'Ожидает запчасти', 'Готов', 'Выдан']
    
    orders = []
    for i in range(1, 51):
        order = {
            'order_id': i,
            'client_id': random.choice(df_clients['client_id']),
            'device_id': random.choice(df_devices['device_id']),
            'issue_type': random.choice(issue_types),
            'status': random.choice(statuses),
            'priority': random.choice(['Низкий', 'Средний', 'Высокий', 'Срочный']),
            'cost_estimate': random.randint(500, 15000),
            'engineer_id': random.randint(1, 10),
            'created_date': fake.date_time_between(start_date='-30d', end_date='now'),
        }
        orders.append(order)
    
    df_orders = pd.DataFrame(orders)
    
    print(f"Сгенерировано заказов: {len(df_orders)}")
    print("\nРаспределение по статусам:")
    print(df_orders['status'].value_counts())
    print("\nРаспределение по приоритетам:")
    print(df_orders['priority'].value_counts())
    
    # Сохранение в XCom
    context['ti'].xcom_push(key='orders_data', value=df_orders.to_json())
    print("\n✓ Данные о заказах сохранены!")
    print("=" * 60)

def generate_spare_parts(**context):
    """Генерация данных о запчастях"""
    print("=" * 60)
    print("Генерация данных о запчастях...")
    print("=" * 60)
    
    part_types = [
        'Экран', 'Батарея', 'Зарядное устройство', 'Материнская плата',
        'Процессор', 'Оперативная память', 'Жесткий диск', 'Камера',
        'Динамик', 'Микрофон', 'Корпус', 'Кнопки'
    ]
    
    spare_parts = []
    for i in range(1, 41):
        part = {
            'part_id': i,
            'name': random.choice(part_types),
            'price': random.randint(200, 8000),
            'quantity_in_stock': random.randint(0, 50),
            'supplier': fake.company(),
            'delivery_days': random.randint(1, 14)
        }
        spare_parts.append(part)
    
    df_parts = pd.DataFrame(spare_parts)
    
    print(f"Сгенерировано запчастей: {len(df_parts)}")
    print("\nЗапчасти на складе (quantity > 0):")
    print(len(df_parts[df_parts['quantity_in_stock'] > 0]))
    print("\nНеобходимо заказать (quantity = 0):")
    print(len(df_parts[df_parts['quantity_in_stock'] == 0]))
    
    context['ti'].xcom_push(key='parts_data', value=df_parts.to_json())
    print("\n✓ Данные о запчастях сохранены!")
    print("=" * 60)

def analyze_repair_data(**context):
    """Анализ данных сервиса ремонта"""
    print("=" * 60)
    print("АНАЛИТИКА СЕРВИСА РЕМОНТА")
    print("=" * 60)
    
    orders_json = context['ti'].xcom_pull(key='orders_data', task_ids='generate_orders')
    parts_json = context['ti'].xcom_pull(key='parts_data', task_ids='generate_parts')
    devices_json = context['ti'].xcom_pull(key='devices_data', task_ids='generate_devices')
    
    df_orders = pd.read_json(orders_json)
    df_parts = pd.read_json(parts_json)
    df_devices = pd.read_json(devices_json)
    
    print("\n📊 СТАТИСТИКА ПО ЗАКАЗАМ:")
    print(f"  Всего заказов: {len(df_orders)}")
    print(f"  Средняя стоимость ремонта: {df_orders['cost_estimate'].mean():.2f} руб.")
    print(f"  Общая выручка: {df_orders['cost_estimate'].sum():.2f} руб.")
    print(f"  Максимальная стоимость: {df_orders['cost_estimate'].max():.2f} руб.")
    print(f"  Минимальная стоимость: {df_orders['cost_estimate'].min():.2f} руб.")
    
    print("\n🔧 ТОП-3 ТИПА НЕИСПРАВНОСТЕЙ:")
    top_issues = df_orders['issue_type'].value_counts().head(3)
    for idx, (issue, count) in enumerate(top_issues.items(), 1):
        print(f"  {idx}. {issue}: {count} заказов")
    
    print("\n📱 РАСПРЕДЕЛЕНИЕ ПО УСТРОЙСТВАМ:")
    device_counts = df_devices['type'].value_counts()
    for device_type, count in device_counts.items():
        print(f"  {device_type}: {count}")
    
    print("\n💰 АНАЛИЗ ЗАПЧАСТЕЙ:")
    print(f"  Всего позиций: {len(df_parts)}")
    print(f"  В наличии: {len(df_parts[df_parts['quantity_in_stock'] > 0])}")
    print(f"  Требуют заказа: {len(df_parts[df_parts['quantity_in_stock'] == 0])}")
    print(f"  Общая стоимость склада: {(df_parts['price'] * df_parts['quantity_in_stock']).sum():.2f} руб.")
    
    print("\n⚡ ПРИОРИТЕТЫ ЗАКАЗОВ:")
    priority_counts = df_orders['priority'].value_counts()
    for priority, count in priority_counts.items():
        print(f"  {priority}: {count} заказов")
    
    print("\n✓ Анализ завершен!")
    print("=" * 60)

def generate_report(**context):
    """Генерация итогового отчета"""
    print("=" * 60)
    print("ГЕНЕРАЦИЯ ИТОГОВОГО ОТЧЕТА")
    print("=" * 60)
    
    orders_json = context['ti'].xcom_pull(key='orders_data', task_ids='generate_orders')
    df_orders = pd.read_json(orders_json)
    
    report = f"""
╔═══════════════════════════════════════════════════════════╗
║         ОТЧЕТ СЕРВИСА РЕМОНТА ТЕХНИКИ                    ║
║         Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}                           ║
╠═══════════════════════════════════════════════════════════╣
║                                                           ║
║  📈 ОСНОВНЫЕ ПОКАЗАТЕЛИ:                                 ║
║  • Обработано заказов: {len(df_orders):<30}║
║  • Общая выручка: {df_orders['cost_estimate'].sum():<35.2f}║
║  • Средний чек: {df_orders['cost_estimate'].mean():<37.2f}║
║                                                           ║
║  ✅ ЗАВЕРШЕННЫЕ ЗАКАЗЫ:                                  ║
║  • Готовы к выдаче: {len(df_orders[df_orders['status'] == 'Готов']):<32}║
║  • Выданы клиентам: {len(df_orders[df_orders['status'] == 'Выдан']):<32}║
║                                                           ║
║  ⏳ ЗАКАЗЫ В РАБОТЕ:                                     ║
║  • В процессе ремонта: {len(df_orders[df_orders['status'] == 'В работе']):<27}║
║  • Ожидают запчасти: {len(df_orders[df_orders['status'] == 'Ожидает запчасти']):<29}║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
    """
    
    print(report)
    print("\n✓ Отчет успешно сформирован!")
    print("=" * 60)

with DAG(
    dag_id='repair_analytics_pipeline',
    default_args={
        'owner': 'repair_service',
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    },
    description='Пайплайн аналитики сервиса ремонта техники',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['repair-service', 'analytics', 'reporting'],
) as dag:
    
    task_gen_clients = PythonOperator(
        task_id='generate_clients',
        python_callable=generate_clients_data,
    )
    
    task_gen_devices = PythonOperator(
        task_id='generate_devices',
        python_callable=generate_devices_data,
    )
    
    task_gen_orders = PythonOperator(
        task_id='generate_orders',
        python_callable=generate_repair_orders,
    )
    
    task_gen_parts = PythonOperator(
        task_id='generate_parts',
        python_callable=generate_spare_parts,
    )
    
    task_analyze = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_repair_data,
    )
    
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )
    
    [task_gen_clients, task_gen_devices] >> task_gen_orders
    
    task_gen_orders >> [task_gen_parts, task_analyze]
    
    task_analyze >> task_report
