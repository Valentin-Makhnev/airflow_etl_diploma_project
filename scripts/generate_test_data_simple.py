"""
Простая генерация тестовых данных без зависимостей от Airflow
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import psycopg2
from pymongo import MongoClient
import json

def generate_customers(n=50):
    """Генерация тестовых данных клиентов"""
    cities = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Казань']
    countries = ['Россия']
    segments = ['Standard', 'Premium', 'VIP']
    
    customers = []
    for i in range(1, n + 1):
        customer = {
            'customer_id': i,
            'first_name': f'Имя_{i}',
            'last_name': f'Фамилия_{i}',
            'email': f'customer{i}@example.com',
            'phone': f'+7916{random.randint(1000000, 9999999)}',
            'city': random.choice(cities),
            'country': random.choice(countries),
            'registration_date': datetime.now() - timedelta(days=random.randint(1, 365)),
            'customer_segment': random.choice(segments),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
            'updated_at': datetime.now() - timedelta(days=random.randint(0, 30))
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_products(n=20):
    """Генерация тестовых данных продуктов"""
    categories = ['Электроника', 'Одежда', 'Книги', 'Бытовая техника', 'Мебель']
    brands = ['Apple', 'Samsung', 'Sony', 'Bosch', 'IKEA', 'Zara', 'Nike']
    
    products = []
    for i in range(1, n + 1):
        product = {
            'product_id': i,
            'product_name': f'Товар {i}',
            'category': random.choice(categories),
            'subcategory': f'Подкатегория {random.randint(1, 3)}',
            'brand': random.choice(brands),
            'unit_price': round(random.uniform(100, 10000), 2),
            'cost_price': round(random.uniform(50, 5000), 2),
            'stock_quantity': random.randint(0, 100),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
            'updated_at': datetime.now() - timedelta(days=random.randint(0, 30))
        }
        products.append(product)
    
    return pd.DataFrame(products)

def generate_orders(customers_df, n=100):
    """Генерация тестовых данных заказов"""
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    payment_methods = ['Карта', 'Наличные', 'Перевод', 'Криптовалюта']
    
    orders = []
    for i in range(1, n + 1):
        customer = customers_df.sample(1).iloc[0]
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        
        order = {
            'order_id': i,
            'customer_id': int(customer['customer_id']),
            'order_date': order_date,
            'order_time': order_date.time(),
            'total_amount': round(random.uniform(1000, 50000), 2),
            'status': random.choice(statuses),
            'payment_method': random.choice(payment_methods),
            'shipping_city': customer['city'],
            'shipping_country': customer['country'],
            'created_at': order_date
        }
        orders.append(order)
    
    return pd.DataFrame(orders)

def generate_order_items(orders_df, products_df):
    """Генерация деталей заказов"""
    order_items = []
    item_id = 1
    
    for _, order in orders_df.iterrows():
        n_items = random.randint(1, 5)
        for _ in range(n_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 3)
            unit_price = product['unit_price']
            
            item = {
                'order_item_id': item_id,
                'order_id': int(order['order_id']),
                'product_id': int(product['product_id']),
                'quantity': quantity,
                'unit_price': unit_price,
                'created_at': order['order_date']
            }
            order_items.append(item)
            item_id += 1
    
    return pd.DataFrame(order_items)

def generate_feedback(customers_df, products_df, n=30):
    """Генерация тестовых отзывов"""
    feedbacks = []
    
    for i in range(1, n + 1):
        customer = customers_df.sample(1).iloc[0]
        product = products_df.sample(1).iloc[0]
        feedback_date = datetime.now() - timedelta(days=random.randint(0, 30))
        
        comments = [
            'Отличный товар!',
            'Хорошее качество',
            'Быстрая доставка',
            'Не понравилось',
            'Можно лучше',
            'Рекомендую!'
        ]
        
        feedback = {
            'feedback_id': f'FB_{i:04d}',
            'customer_id': int(customer['customer_id']),
            'customer_email': customer['email'],
            'product_id': int(product['product_id']),
            'product_name': product['product_name'],
            'rating': random.randint(1, 5),
            'comment': random.choice(comments),
            'feedback_date': feedback_date,
            'helpful_votes': random.randint(0, 50),
            'verified_purchase': random.choice([True, False]),
            'created_at': feedback_date
        }
        feedbacks.append(feedback)
    
    return feedbacks

def connect_postgres(host='postgres-source', port=5432, database='source_db', 
                     user='source_user', password='source_password123'):
    """Подключение к PostgreSQL"""
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    return conn

def load_to_postgres(df, table_name, conn):
    """Загрузка данных в PostgreSQL"""
    cursor = conn.cursor()
    
    # Очистка таблицы
    cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
    
    # Для таблицы order_items исключаем generated колонку total_price
    if table_name == 'order_items':
        # Удаляем total_price из данных, если он есть
        if 'total_price' in df.columns:
            df_to_insert = df.drop('total_price', axis=1)
        else:
            df_to_insert = df
    else:
        df_to_insert = df
    
    # Вставка данных
    for _, row in df_to_insert.iterrows():
        columns = ', '.join(row.index)
        placeholders = ', '.join(['%s'] * len(row))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        values = []
        for val in row.values:
            if isinstance(val, datetime):
                values.append(val)
            elif pd.isna(val):
                values.append(None)
            else:
                values.append(val)
        
        try:
            cursor.execute(query, values)
        except Exception as e:
            print(f"❌ Ошибка при вставке в {table_name}: {e}")
            print(f"   Запрос: {query}")
            print(f"   Значения: {values}")
            raise
    
    conn.commit()
    cursor.close()
    print(f"✅ Загружено {len(df_to_insert)} записей в {table_name}")

def load_to_mongodb(data, collection_name, database='source_mongo_db'):
    """Загрузка данных в MongoDB"""
    try:
        client = MongoClient(
            'mongodb://mongo_user:mongo_password123@mongodb:27017/',
            authSource='admin',
            authMechanism='SCRAM-SHA-256',
            serverSelectionTimeoutMS=5000
        )
        
        # Проверяем подключение
        client.admin.command('ping')
        
        db = client[database]
        collection = db[collection_name]
        
        # Очистка коллекции
        collection.delete_many({})
        
        # Вставка данных
        if data:
            collection.insert_many(data)
        
        print(f"✅ Загружено {len(data)} документов в {collection_name}")
        client.close()
    except Exception as e:
        print(f"⚠ Предупреждение при загрузке в MongoDB: {e}")
        print("   Продолжаем выполнение без MongoDB...")

def main():
    """Основная функция генерации данных"""
    print("=" * 80)
    print("🧪 ГЕНЕРАЦИЯ ТЕСТОВЫХ ДАННЫХ (простая версия)")
    print("=" * 80)
    
    try:
        # Подключаемся к PostgreSQL
        print("\nПодключение к PostgreSQL...")
        pg_conn = connect_postgres()
        print("✅ Подключено к PostgreSQL")
        
        # Генерация данных
        print("\n1. Генерация данных о клиентах...")
        customers_df = generate_customers(20)
        print(f"   Сгенерировано клиентов: {len(customers_df)}")
        
        print("\n2. Генерация данных о продуктах...")
        products_df = generate_products(15)
        print(f"   Сгенерировано продуктов: {len(products_df)}")
        
        print("\n3. Генерация данных о заказах...")
        orders_df = generate_orders(customers_df, 30)
        print(f"   Сгенерировано заказов: {len(orders_df)}")
        
        print("\n4. Генерация деталей заказов...")
        order_items_df = generate_order_items(orders_df, products_df)
        print(f"   Сгенерировано позиций заказов: {len(order_items_df)}")
        
        print("\n5. Генерация отзывов...")
        feedback_data = generate_feedback(customers_df, products_df, 20)
        print(f"   Сгенерировано отзывов: {len(feedback_data)}")
        
        # Загрузка в PostgreSQL
        print("\n" + "=" * 80)
        print("📥 ЗАГРУЗКА В POSTGRESQL")
        print("=" * 80)
        
        load_to_postgres(customers_df, 'customers', pg_conn)
        load_to_postgres(products_df, 'products', pg_conn)
        load_to_postgres(orders_df, 'orders', pg_conn)
        load_to_postgres(order_items_df, 'order_items', pg_conn)
        
        # Закрываем соединение с PostgreSQL
        pg_conn.close()
        
        # Загрузка в MongoDB
        print("\n" + "=" * 80)
        print("📥 ЗАГРУЗКА В MONGODB")
        print("=" * 80)
        
        load_to_mongodb(feedback_data, 'customer_feedback')
        
        print("\n" + "=" * 80)
        print("✅ ГЕНЕРАЦИЯ ДАННЫХ ЗАВЕРШЕНА")
        print("=" * 80)
        
        # Показываем статистику
        print("\n📊 СТАТИСТИКА СГЕНЕРИРОВАННЫХ ДАННЫХ:")
        print(f"   Клиентов: {len(customers_df)}")
        print(f"   Продуктов: {len(products_df)}")
        print(f"   Заказов: {len(orders_df)}")
        print(f"   Позиций заказов: {len(order_items_df)}")
        print(f"   Отзывов: {len(feedback_data)}")
        
        # Проверка данных
        print("\n🔍 ПРОВЕРКА ДАННЫХ В БАЗАХ:")
        
        # Проверяем PostgreSQL
        try:
            pg_conn = connect_postgres()
            cursor = pg_conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM customers")
            cust_count = cursor.fetchone()[0]
            print(f"   PostgreSQL - customers: {cust_count} записей")
            
            cursor.execute("SELECT COUNT(*) FROM products")
            prod_count = cursor.fetchone()[0]
            print(f"   PostgreSQL - products: {prod_count} записей")
            
            cursor.execute("SELECT COUNT(*) FROM orders")
            order_count = cursor.fetchone()[0]
            print(f"   PostgreSQL - orders: {order_count} записей")
            
            cursor.execute("SELECT COUNT(*) FROM order_items")
            items_count = cursor.fetchone()[0]
            print(f"   PostgreSQL - order_items: {items_count} записей")
            
            cursor.close()
            pg_conn.close()
        except Exception as e:
            print(f"   ❌ Ошибка проверки PostgreSQL: {e}")
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
    