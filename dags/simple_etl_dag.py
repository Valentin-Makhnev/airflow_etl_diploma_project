"""
Упрощенный ETL DAG без плагинов
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(2),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    description='Упрощенный ETL пайплайн без плагинов',
    schedule_interval='0 9 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'simple', 'diploma'],
)

def extract_data(**kwargs):
    """Извлечение данных без плагинов"""
    print("=" * 60)
    print("📥 ИЗВЛЕЧЕНИЕ ДАННЫХ")
    print("=" * 60)
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.providers.mongo.hooks.mongo import MongoHook
        
        execution_date = kwargs.get('execution_date', datetime.now())
        ti = kwargs.get('ti')
        
        print(f"Дата выполнения: {execution_date}")
        
        # 1. Извлекаем из PostgreSQL Source
        print("\n1. Извлечение из PostgreSQL Source:")
        pg_source_hook = PostgresHook(postgres_conn_id='postgres_source')
        
        # Клиенты
        customers_df = pg_source_hook.get_pandas_df("SELECT * FROM customers")
        print(f"   Клиенты: {len(customers_df)} записей")
        
        # Продукты
        products_df = pg_source_hook.get_pandas_df("SELECT * FROM products")
        print(f"   Продукты: {len(products_df)} записей")
        
        # Заказы (за последние 7 дней для демонстрации)
        start_date = (execution_date - timedelta(days=7)).strftime('%Y-%m-%d')
        orders_query = f"""
        SELECT * FROM orders 
        WHERE order_date >= '{start_date}'
        """
        orders_df = pg_source_hook.get_pandas_df(orders_query)
        print(f"   Заказы: {len(orders_df)} записей")
        
        # 2. Извлекаем из MongoDB
        print("\n2. Извлечение из MongoDB:")
        mongo_hook = MongoHook(conn_id='mongodb_source')
        client = mongo_hook.get_conn()
        
        db = client['source_mongo_db']
        feedback_data = list(db.customer_feedback.find().limit(100))  # Ограничим для демонстрации
        
        # Конвертируем ObjectId
        for doc in feedback_data:
            doc['_id'] = str(doc['_id'])
        
        feedback_df = pd.DataFrame(feedback_data)
        print(f"   Отзывы: {len(feedback_df)} записей")
        
        # Сохраняем в XCom
        if ti:
            ti.xcom_push(key='customers_df', value=customers_df.to_json())
            ti.xcom_push(key='products_df', value=products_df.to_json())
            ti.xcom_push(key='orders_df', value=orders_df.to_json())
            ti.xcom_push(key='feedback_df', value=feedback_df.to_json())
        
        print(f"\n✅ Извлечение завершено успешно!")
        
        return {
            'status': 'success',
            'customers': len(customers_df),
            'products': len(products_df),
            'orders': len(orders_df),
            'feedback': len(feedback_df)
        }
        
    except Exception as e:
        error_msg = f"❌ Ошибка извлечения: {e}"
        print(error_msg)
        return {'status': 'error', 'error': str(e)}

def transform_data(**kwargs):
    """Трансформация данных без плагинов"""
    print("=" * 60)
    print("🔄 ТРАНСФОРМАЦИЯ ДАННЫХ")
    print("=" * 60)
    
    try:
        ti = kwargs.get('ti')
        execution_date = kwargs.get('execution_date', datetime.now())
        
        # Получаем данные из XCom
        customers_json = ti.xcom_pull(task_ids='extract_data', key='customers_df')
        products_json = ti.xcom_pull(task_ids='extract_data', key='products_df')
        orders_json = ti.xcom_pull(task_ids='extract_data', key='orders_df')
        feedback_json = ti.xcom_pull(task_ids='extract_data', key='feedback_df')
        
        # Конвертируем обратно в DataFrame
        customers_df = pd.read_json(customers_json) if customers_json else pd.DataFrame()
        products_df = pd.read_json(products_json) if products_json else pd.DataFrame()
        orders_df = pd.read_json(orders_json) if orders_json else pd.DataFrame()
        feedback_df = pd.read_json(feedback_json) if feedback_json else pd.DataFrame()
        
        print(f"📥 Получено для трансформации:")
        print(f"   Клиенты: {len(customers_df)}")
        print(f"   Продукты: {len(products_df)}")
        print(f"   Заказы: {len(orders_df)}")
        print(f"   Отзывы: {len(feedback_df)}")
        
        # Простая трансформация
        transformations = {}
        
        if not customers_df.empty:
            # Очистка клиентов
            customers_df['city'] = customers_df['city'].fillna('Не указан')
            customers_df['country'] = customers_df['country'].fillna('Россия')
            customers_df['customer_segment'] = customers_df['customer_segment'].fillna('Standard')
            
            # Нормализация
            customers_df['first_name'] = customers_df['first_name'].str.strip().str.title()
            customers_df['last_name'] = customers_df['last_name'].str.strip().str.title()
            customers_df['email'] = customers_df['email'].str.lower().str.strip()
            
            transformations['customers'] = customers_df
            print(f"✅ Клиенты трансформированы")
        
        if not products_df.empty:
            # Очистка продуктов
            products_df['category'] = products_df['category'].fillna('Другое')
            products_df['brand'] = products_df['brand'].fillna('Неизвестно')
            products_df['unit_price'] = pd.to_numeric(products_df['unit_price'], errors='coerce').fillna(0)
            
            transformations['products'] = products_df
            print(f"✅ Продукты трансформированы")
        
        if not orders_df.empty:
            # Очистка заказов
            orders_df['status'] = orders_df['status'].fillna('Pending')
            orders_df['payment_method'] = orders_df['payment_method'].fillna('Не указан')
            orders_df['shipping_city'] = orders_df['shipping_city'].fillna('Не указан')
            
            # Конвертация дат
            orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
            orders_df['total_amount'] = pd.to_numeric(orders_df['total_amount'], errors='coerce').fillna(0)
            
            transformations['orders'] = orders_df
            print(f"✅ Заказы трансформированы")
        
        if not feedback_df.empty:
            # Очистка отзывов
            if 'rating' in feedback_df.columns:
                feedback_df['rating'] = pd.to_numeric(feedback_df['rating'], errors='coerce').clip(1, 5).fillna(3)
            
            transformations['feedback'] = feedback_df
            print(f"✅ Отзывы трансформированы")
        
        # Сохраняем трансформированные данные в XCom
        if ti:
            for key, df in transformations.items():
                ti.xcom_push(key=f'transformed_{key}', value=df.to_json())
        
        print(f"\n✅ Трансформация завершена!")
        print(f"📊 Трансформировано таблиц: {len(transformations)}")
        
        return {
            'status': 'success',
            'transformed_tables': list(transformations.keys()),
            'total_records': sum(len(df) for df in transformations.values())
        }
        
    except Exception as e:
        error_msg = f"❌ Ошибка трансформации: {e}"
        print(error_msg)
        return {'status': 'error', 'error': str(e)}

def load_to_analytics(**kwargs):
    """Загрузка в аналитическую БД без плагинов"""
    print("=" * 60)
    print("📊 ЗАГРУЗКА В АНАЛИТИЧЕСКУЮ БД")
    print("=" * 60)
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ti = kwargs.get('ti')
        execution_date = kwargs.get('execution_date', datetime.now())
        
        # Получаем трансформированные данные
        orders_json = ti.xcom_pull(task_ids='transform_data', key='transformed_orders')
        orders_df = pd.read_json(orders_json) if orders_json else pd.DataFrame()
        
        feedback_json = ti.xcom_pull(task_ids='transform_data', key='transformed_feedback')
        feedback_df = pd.read_json(feedback_json) if feedback_json else pd.DataFrame()
        
        if not orders_df.empty:
            # Расчет метрик
            total_orders = len(orders_df)
            total_revenue = orders_df['total_amount'].sum()
            avg_order_value = orders_df['total_amount'].mean() if total_orders > 0 else 0
            
            # Активные клиенты
            active_customers = orders_df['customer_id'].nunique()
            
            # Самый популярный город
            if 'shipping_city' in orders_df.columns:
                city_counts = orders_df['shipping_city'].value_counts()
                top_city = city_counts.index[0] if len(city_counts) > 0 else 'Не указан'
            else:
                top_city = 'Не указан'
            
            # Средний рейтинг
            avg_rating = 0
            if not feedback_df.empty and 'rating' in feedback_df.columns:
                avg_rating = feedback_df['rating'].mean()
            
            print(f"📈 РАССЧИТАННЫЕ МЕТРИКИ:")
            print(f"   Заказы: {total_orders}")
            print(f"   Выручка: {total_revenue:.2f}")
            print(f"   Средний чек: {avg_order_value:.2f}")
            print(f"   Клиентов: {active_customers}")
            print(f"   Топ город: {top_city}")
            print(f"   Средний рейтинг: {avg_rating:.2f}")
            
            # Загрузка в аналитическую БД
            analytics_hook = PostgresHook(postgres_conn_id='postgres_analytics')
            
            # Проверяем существование таблицы
            table_exists = analytics_hook.get_first("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'daily_business_analytics'
                )
            """)[0]
            
            if not table_exists:
                print("⚠ Таблица daily_business_analytics не существует, создаем...")
                # Создаем таблицу
                create_table_sql = """
                CREATE TABLE daily_business_analytics (
                    analytics_date DATE PRIMARY KEY,
                    total_orders INTEGER,
                    total_revenue DECIMAL(12, 2),
                    avg_order_value DECIMAL(10, 2),
                    active_customers INTEGER,
                    top_city VARCHAR(100),
                    avg_customer_rating DECIMAL(3, 2),
                    data_source VARCHAR(50),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                analytics_hook.run(create_table_sql)
            
            # Вставляем данные
            insert_sql = """
            INSERT INTO daily_business_analytics (
                analytics_date, total_orders, total_revenue, 
                avg_order_value, active_customers, top_city,
                avg_customer_rating, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (analytics_date) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_revenue = EXCLUDED.total_revenue,
                avg_order_value = EXCLUDED.avg_order_value,
                active_customers = EXCLUDED.active_customers,
                top_city = EXCLUDED.top_city,
                avg_customer_rating = EXCLUDED.avg_customer_rating,
                data_source = EXCLUDED.data_source,
                processed_at = CURRENT_TIMESTAMP
            """
            
            analytics_hook.run(insert_sql, parameters=(
                execution_date.date(),
                total_orders,
                float(total_revenue),
                float(avg_order_value),
                active_customers,
                top_city,
                float(avg_rating),
                'simple_etl_dag'
            ))
            
            print(f"\n✅ Данные загружены в аналитическую БД")
        else:
            print("⚠ Нет данных о заказах для загрузки")
        
        return {'status': 'success', 'metrics_loaded': 1}
        
    except Exception as e:
        error_msg = f"❌ Ошибка загрузки в аналитическую БД: {e}"
        print(error_msg)
        return {'status': 'error', 'error': str(e)}

def load_to_dwh_simple(**kwargs):
    """Простая загрузка в DWH без SCD Type 2"""
    print("=" * 60)
    print("🏗 ЗАГРУЗКА В DWH (УПРОЩЕННАЯ)")
    print("=" * 60)
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ti = kwargs.get('ti')
        execution_date = kwargs.get('execution_date', datetime.now())
        
        # Получаем трансформированные данные
        customers_json = ti.xcom_pull(task_ids='transform_data', key='transformed_customers')
        products_json = ti.xcom_pull(task_ids='transform_data', key='transformed_products')
        orders_json = ti.xcom_pull(task_ids='transform_data', key='transformed_orders')
        
        customers_df = pd.read_json(customers_json) if customers_json else pd.DataFrame()
        products_df = pd.read_json(products_json) if products_json else pd.DataFrame()
        orders_df = pd.read_json(orders_json) if orders_json else pd.DataFrame()
        
        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        
        results = {
            'customers_loaded': 0,
            'products_loaded': 0,
            'orders_loaded': 0
        }
        
        # 1. Загрузка клиентов (упрощенная, без SCD)
        if not customers_df.empty:
            print("\n1. Загрузка клиентов в DWH:")
            
            # Создаем таблицу, если не существует
            create_customers_table = """
            CREATE TABLE IF NOT EXISTS dim_customers_simple (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                city VARCHAR(100),
                country VARCHAR(100),
                customer_segment VARCHAR(50),
                registration_date DATE,
                load_date DATE DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            dwh_hook.run(create_customers_table)
            
            # Загружаем клиентов
            for _, customer in customers_df.iterrows():
                insert_customer = """
                INSERT INTO dim_customers_simple (
                    customer_id, first_name, last_name, email,
                    city, country, customer_segment, registration_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    customer_segment = EXCLUDED.customer_segment
                """
                
                try:
                    dwh_hook.run(insert_customer, parameters=(
                        int(customer['customer_id']),
                        str(customer.get('first_name', '')),
                        str(customer.get('last_name', '')),
                        str(customer.get('email', '')),
                        str(customer.get('city', 'Не указан')),
                        str(customer.get('country', 'Россия')),
                        str(customer.get('customer_segment', 'Standard')),
                        customer.get('registration_date')
                    ))
                    results['customers_loaded'] += 1
                except:
                    pass
            
            print(f"   ✅ Клиентов загружено: {results['customers_loaded']}")
        
        # 2. Загрузка продуктов (упрощенная)
        if not products_df.empty:
            print("\n2. Загрузка продуктов в DWH:")
            
            create_products_table = """
            CREATE TABLE IF NOT EXISTS dim_products_simple (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(255),
                category VARCHAR(100),
                brand VARCHAR(100),
                unit_price DECIMAL(10, 2),
                stock_quantity INTEGER,
                load_date DATE DEFAULT CURRENT_DATE
            )
            """
            dwh_hook.run(create_products_table)
            
            for _, product in products_df.iterrows():
                insert_product = """
                INSERT INTO dim_products_simple (
                    product_id, product_name, category, brand,
                    unit_price, stock_quantity
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    unit_price = EXCLUDED.unit_price,
                    stock_quantity = EXCLUDED.stock_quantity
                """
                
                try:
                    dwh_hook.run(insert_product, parameters=(
                        int(product['product_id']),
                        str(product.get('product_name', '')),
                        str(product.get('category', 'Другое')),
                        str(product.get('brand', 'Неизвестно')),
                        float(product.get('unit_price', 0)),
                        int(product.get('stock_quantity', 0))
                    ))
                    results['products_loaded'] += 1
                except:
                    pass
            
            print(f"   ✅ Продуктов загружено: {results['products_loaded']}")
        
        # 3. Загрузка заказов (упрощенная)
        if not orders_df.empty:
            print("\n3. Загрузка заказов в DWH:")
            
            create_orders_table = """
            CREATE TABLE IF NOT EXISTS fact_orders_simple (
                order_id INTEGER,
                customer_id INTEGER,
                product_id INTEGER,
                order_date DATE,
                total_amount DECIMAL(12, 2),
                status VARCHAR(50),
                payment_method VARCHAR(50),
                shipping_city VARCHAR(100),
                load_date DATE DEFAULT CURRENT_DATE,
                PRIMARY KEY (order_id, load_date)
            )
            """
            dwh_hook.run(create_orders_table)
            
            # Предположим, что у нас есть product_id (в реальности нужно из order_items)
            product_id_default = 1
            
            for _, order in orders_df.iterrows():
                insert_order = """
                INSERT INTO fact_orders_simple (
                    order_id, customer_id, product_id,
                    order_date, total_amount, status,
                    payment_method, shipping_city
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id, load_date) DO NOTHING
                """
                
                try:
                    order_date = order.get('order_date')
                    if pd.isna(order_date):
                        order_date = execution_date.date()
                    
                    dwh_hook.run(insert_order, parameters=(
                        int(order['order_id']),
                        int(order.get('customer_id', 0)),
                        product_id_default,
                        order_date,
                        float(order.get('total_amount', 0)),
                        str(order.get('status', 'Pending')),
                        str(order.get('payment_method', 'Не указан')),
                        str(order.get('shipping_city', 'Не указан'))
                    ))
                    results['orders_loaded'] += 1
                except Exception as e:
                    print(f"   ⚠ Ошибка загрузки заказа {order['order_id']}: {e}")
            
            print(f"   ✅ Заказов загружено: {results['orders_loaded']}")
        
        print(f"\n✅ Загрузка в DWH завершена")
        print(f"📊 Результаты: {results}")
        
        return {'status': 'success', 'results': results}
        
    except Exception as e:
        error_msg = f"❌ Ошибка загрузки в DWH: {e}"
        print(error_msg)
        return {'status': 'error', 'error': str(e)}

def validate_results(**kwargs):
    """Валидация результатов"""
    print("=" * 60)
    print("🔍 ВАЛИДАЦИЯ РЕЗУЛЬТАТОВ")
    print("=" * 60)
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ti = kwargs.get('ti')
        
        # Получаем результаты всех задач
        extract_result = ti.xcom_pull(task_ids='extract_data')
        transform_result = ti.xcom_pull(task_ids='transform_data')
        load_analytics_result = ti.xcom_pull(task_ids='load_to_analytics')
        load_dwh_result = ti.xcom_pull(task_ids='load_to_dwh_simple')
        
        print("📋 РЕЗУЛЬТАТЫ ВАЛИДАЦИИ:")
        
        checks_passed = 0
        checks_failed = 0
        
        # Проверка извлечения
        if extract_result and extract_result.get('status') == 'success':
            print(f"✅ Extract: Успешно")
            checks_passed += 1
        else:
            print(f"❌ Extract: Ошибка")
            checks_failed += 1
        
        # Проверка трансформации
        if transform_result and transform_result.get('status') == 'success':
            print(f"✅ Transform: Успешно")
            checks_passed += 1
        else:
            print(f"❌ Transform: Ошибка")
            checks_failed += 1
        
        # Проверка загрузки в аналитику
        if load_analytics_result and load_analytics_result.get('status') == 'success':
            print(f"✅ Analytics Load: Успешно")
            checks_passed += 1
        else:
            print(f"❌ Analytics Load: Ошибка")
            checks_failed += 1
        
        # Проверка загрузки в DWH
        if load_dwh_result and load_dwh_result.get('status') == 'success':
            print(f"✅ DWH Load: Успешно")
            checks_passed += 1
        else:
            print(f"❌ DWH Load: Ошибка")
            checks_failed += 1
        
        # Проверка данных в целевых БД
        try:
            print("\n📊 ПРОВЕРКА ДАННЫХ В ЦЕЛЕВЫХ БД:")
            
            # Проверка аналитической БД
            analytics_hook = PostgresHook(postgres_conn_id='postgres_analytics')
            analytics_count = analytics_hook.get_first("SELECT COUNT(*) FROM daily_business_analytics")[0]
            print(f"   Аналитическая БД: {analytics_count} записей")
            
            # Проверка DWH
            dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
            customers_count = dwh_hook.get_first("SELECT COUNT(*) FROM dim_customers_simple")[0]
            products_count = dwh_hook.get_first("SELECT COUNT(*) FROM dim_products_simple")[0]
            orders_count = dwh_hook.get_first("SELECT COUNT(*) FROM fact_orders_simple")[0]
            
            print(f"   DWH - Клиенты: {customers_count}")
            print(f"   DWH - Продукты: {products_count}")
            print(f"   DWH - Заказы: {orders_count}")
            
            checks_passed += 1
        except Exception as e:
            print(f"   ⚠ Проверка целевых БД: {e}")
            checks_failed += 1
        
        print(f"\n📊 ИТОГИ ВАЛИДАЦИИ:")
        print(f"   Всего проверок: {checks_passed + checks_failed}")
        print(f"   Успешно: {checks_passed}")
        print(f"   Ошибки: {checks_failed}")
        
        if checks_failed == 0:
            print(f"\n🎉 ETL ПРОЦЕСС УСПЕШНО ЗАВЕРШЕН!")
            return {'status': 'success', 'checks_passed': checks_passed}
        else:
            print(f"\n⚠ ETL ПРОЦЕСС ЗАВЕРШЕН С ОШИБКАМИ")
            return {'status': 'warning', 'checks_passed': checks_passed, 'checks_failed': checks_failed}
        
    except Exception as e:
        error_msg = f"❌ Ошибка валидации: {e}"
        print(error_msg)
        return {'status': 'error', 'error': str(e)}

# Создаем операторы
start_task = DummyOperator(task_id='start_etl', dag=dag)
end_task = DummyOperator(task_id='end_etl', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
    provide_context=True,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
    provide_context=True,
)

load_analytics_task = PythonOperator(
    task_id='load_to_analytics',
    python_callable=load_to_analytics,
    dag=dag,
    provide_context=True,
)

load_dwh_task = PythonOperator(
    task_id='load_to_dwh_simple',
    python_callable=load_to_dwh_simple,
    dag=dag,
    provide_context=True,
)

validate_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
    provide_context=True,
    trigger_rule='all_done',
)

# Настраиваем зависимости
start_task >> extract_task >> transform_task
transform_task >> load_analytics_task
transform_task >> load_dwh_task
[load_analytics_task, load_dwh_task] >> validate_task >> end_task

print("✅ DAG 'simple_etl_dag' создан успешно!")

