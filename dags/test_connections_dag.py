"""
Тестовый DAG для проверки подключений
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}

dag = DAG(
    'test_connections_dag',
    default_args=default_args,
    description='Тестирование подключений к БД',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
)

def test_postgres_source(**context):
    """Тест подключения к PostgreSQL Source"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        print("🔧 Тестируем подключение к PostgreSQL Source...")
        hook = PostgresHook(postgres_conn_id='postgres_source')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Проверяем таблицы
        cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"✅ PostgreSQL Source подключен успешно!")
        print(f"📋 Найдено таблиц: {len(tables)}")
        
        for table in tables:
            print(f"   - {table[0]}")
        
        cursor.close()
        conn.close()
        
        return {'status': 'success', 'tables': [t[0] for t in tables]}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL Source: {e}")
        return {'status': 'error', 'error': str(e)}

def test_postgres_analytics(**context):
    """Тест подключения к PostgreSQL Analytics"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        print("🔧 Тестируем подключение к PostgreSQL Analytics...")
        hook = PostgresHook(postgres_conn_id='postgres_analytics')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print(f"✅ PostgreSQL Analytics подключен успешно!")
        print(f"📋 Версия PostgreSQL: {version[0]}")
        
        cursor.close()
        conn.close()
        
        return {'status': 'success', 'version': version[0]}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL Analytics: {e}")
        return {'status': 'error', 'error': str(e)}

def test_postgres_dwh(**context):
    """Тест подключения к PostgreSQL DWH"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        print("🔧 Тестируем подключение к PostgreSQL DWH...")
        hook = PostgresHook(postgres_conn_id='postgres_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print(f"✅ PostgreSQL DWH подключен успешно!")
        print(f"📋 Версия PostgreSQL: {version[0]}")
        
        cursor.close()
        conn.close()
        
        return {'status': 'success', 'version': version[0]}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL DWH: {e}")
        return {'status': 'error', 'error': str(e)}

def test_mongodb(**context):
    """Тест подключения к MongoDB"""
    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook
        
        print("🔧 Тестируем подключение к MongoDB...")
        hook = MongoHook(conn_id='mongodb_source')
        
        # Проверяем подключение
        client = hook.get_conn()
        
        # Получаем список баз данных
        dbs = client.list_database_names()
        
        print(f"✅ MongoDB подключен успешно!")
        print(f"📋 Найдено баз данных: {len(dbs)}")
        
        for db in dbs[:5]:  # Показываем первые 5
            print(f"   - {db}")
        
        return {'status': 'success', 'databases': dbs}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return {'status': 'error', 'error': str(e)}

def check_data_in_sources(**context):
    """Проверка наличия данных в источниках"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        print("📊 Проверяем данные в источниках...")
        
        # PostgreSQL Source
        hook = PostgresHook(postgres_conn_id='postgres_source')
        
        # Проверяем таблицу customers
        customers_count = hook.get_first("SELECT COUNT(*) FROM customers")[0]
        print(f"   Customers: {customers_count} записей")
        
        # Проверяем таблицу products
        products_count = hook.get_first("SELECT COUNT(*) FROM products")[0]
        print(f"   Products: {products_count} записей")
        
        # Проверяем таблицу orders
        orders_count = hook.get_first("SELECT COUNT(*) FROM orders")[0]
        print(f"   Orders: {orders_count} записей")
        
        return {
            'status': 'success',
            'customers': customers_count,
            'products': products_count,
            'orders': orders_count
        }
        
    except Exception as e:
        print(f"❌ Ошибка проверки данных: {e}")
        return {'status': 'error', 'error': str(e)}

# Создаем операторы
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

test_postgres_source_task = PythonOperator(
    task_id='test_postgres_source',
    python_callable=test_postgres_source,
    dag=dag,
)

test_postgres_analytics_task = PythonOperator(
    task_id='test_postgres_analytics',
    python_callable=test_postgres_analytics,
    dag=dag,
)

test_postgres_dwh_task = PythonOperator(
    task_id='test_postgres_dwh',
    python_callable=test_postgres_dwh,
    dag=dag,
)

test_mongodb_task = PythonOperator(
    task_id='test_mongodb',
    python_callable=test_mongodb,
    dag=dag,
)

check_data_task = PythonOperator(
    task_id='check_data_in_sources',
    python_callable=check_data_in_sources,
    dag=dag,
)

# Настраиваем зависимости
start >> [
    test_postgres_source_task,
    test_postgres_analytics_task,
    test_postgres_dwh_task,
    test_mongodb_task
] >> check_data_task >> end

print("✅ DAG 'test_connections_dag' создан успешно!")