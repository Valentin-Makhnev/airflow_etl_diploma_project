"""
Простой тест MongoDB для дипломного проекта
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

def test_mongodb_connection():
    """Тестирование подключения к MongoDB"""
    print("=" * 60)
    print("🔍 ТЕСТ ПОДКЛЮЧЕНИЯ К MONGODB")
    print("=" * 60)
    
    try:
        # Импортируем здесь, чтобы избежать ошибок импорта на уровне DAG
        from pymongo import MongoClient
        import json
        
        # Конфигурация подключения
        config = {
            'host': 'mongodb',
            'port': 27017,
            'username': 'mongo_user',
            'password': 'mongo_password123',
            'auth_source': 'admin',
            'database': 'source_mongo_db'
        }
        
        print(f"Конфигурация подключения:")
        for key, value in config.items():
            if key != 'password':
                print(f"  {key}: {value}")
        
        # Создаем строку подключения
        connection_string = f"mongodb://{config['username']}:{config['password']}@{config['host']}:{config['port']}/"
        
        print(f"\nПодключаемся к MongoDB...")
        
        # Подключаемся к MongoDB
        client = MongoClient(
            connection_string,
            authSource=config['auth_source'],
            authMechanism='SCRAM-SHA-256',
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        # Тест 1: Ping
        print("\n1. Проверка ping...")
        try:
            ping_result = client.admin.command('ping')
            print(f"   ✅ Ping успешен: {ping_result}")
        except Exception as e:
            print(f"   ❌ Ошибка ping: {e}")
            client.close()
            return
        
        # Тест 2: Список баз данных
        print("\n2. Получение списка баз данных...")
        try:
            dbs = client.list_database_names()
            print(f"   📊 Найдено баз данных: {len(dbs)}")
            for db_name in dbs:
                print(f"      - {db_name}")
        except Exception as e:
            print(f"   ❌ Ошибка получения списка баз: {e}")
        
        # Тест 3: Проверка базы source_mongo_db
        print(f"\n3. Проверка базы {config['database']}...")
        if config['database'] in dbs:
            db = client[config['database']]
            collections = db.list_collection_names()
            print(f"   📁 Коллекций найдено: {len(collections)}")
            for collection in collections:
                print(f"      - {collection}")
            
            # Тест 4: Проверка коллекции customer_feedback
            print(f"\n4. Проверка коллекции customer_feedback...")
            if 'customer_feedback' in collections:
                count = db.customer_feedback.count_documents({})
                print(f"   📝 Документов в customer_feedback: {count}")
                
                if count > 0:
                    print(f"\n5. Пример документа:")
                    sample = db.customer_feedback.find_one()
                    for key, value in sample.items():
                        if key != '_id' and key != 'password':
                            value_str = str(value)
                            if len(value_str) > 50:
                                value_str = value_str[:50] + "..."
                            print(f"   {key}: {value_str}")
            else:
                print(f"   ⚠ Коллекция customer_feedback не найдена")
        else:
            print(f"   ⚠ База данных {config['database']} не найдена")
        
        client.close()
        print("\n" + "=" * 60)
        print("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

def check_and_create_test_data():
    """Проверка и создание тестовых данных в MongoDB"""
    print("=" * 60)
    print("📊 ПРОВЕРКА И СОЗДАНИЕ ТЕСТОВЫХ ДАННЫХ")
    print("=" * 60)
    
    try:
        from pymongo import MongoClient
        from datetime import datetime
        
        # Подключаемся к MongoDB
        client = MongoClient(
            'mongodb://mongo_user:mongo_password123@mongodb:27017/',
            authSource='admin',
            authMechanism='SCRAM-SHA-256'
        )
        
        # Используем базу source_mongo_db
        db = client['source_mongo_db']
        
        # Проверяем существование коллекции
        if 'customer_feedback' not in db.list_collection_names():
            print("Коллекция customer_feedback не найдена, создаем...")
            db.create_collection('customer_feedback')
            print("✅ Коллекция создана")
        
        # Проверяем количество документов
        count = db.customer_feedback.count_documents({})
        print(f"Текущее количество документов: {count}")
        
        # Если документов мало, добавляем тестовые
        if count < 2:
            print("Добавляем тестовые данные...")
            
            test_data = [
                {
                    'feedback_id': 'FB_2024_001',
                    'customer_id': 1001,
                    'customer_name': 'Иван Иванов',
                    'customer_email': 'ivanov@example.com',
                    'product_id': 101,
                    'product_name': 'Ноутбук Dell XPS 13',
                    'rating': 5,
                    'comment': 'Отличный ноутбук, быстрый и удобный!',
                    'feedback_date': datetime.now(),
                    'created_at': datetime.now(),
                    'verified_purchase': True
                },
                {
                    'feedback_id': 'FB_2024_002',
                    'customer_id': 1002,
                    'customer_name': 'Мария Петрова',
                    'customer_email': 'petrova@example.com',
                    'product_id': 102,
                    'product_name': 'Смартфон iPhone 15',
                    'rating': 4,
                    'comment': 'Хороший телефон, но дорогой',
                    'feedback_date': datetime.now(),
                    'created_at': datetime.now(),
                    'verified_purchase': True
                },
                {
                    'feedback_id': 'FB_2024_003',
                    'customer_id': 1003,
                    'customer_name': 'Алексей Сидоров',
                    'customer_email': 'sidorov@example.com',
                    'product_id': 103,
                    'product_name': 'Наушники Sony WH-1000XM4',
                    'rating': 5,
                    'comment': 'Лучшие наушники на рынке!',
                    'feedback_date': datetime.now(),
                    'created_at': datetime.now(),
                    'verified_purchase': True
                }
            ]
            
            # Вставляем данные
            result = db.customer_feedback.insert_many(test_data)
            print(f"✅ Добавлено {len(result.inserted_ids)} документов")
            
            # Проверяем
            new_count = db.customer_feedback.count_documents({})
            print(f"Общее количество документов: {new_count}")
        else:
            print("✅ В коллекции уже достаточно данных")
        
        # Показываем статистику
        print(f"\n📈 Статистика:")
        print(f"   Всего документов: {db.customer_feedback.count_documents({})}")
        
        # Распределение по рейтингам
        print(f"   Распределение по рейтингам:")
        for rating in range(1, 6):
            count_by_rating = db.customer_feedback.count_documents({'rating': rating})
            print(f"     Рейтинг {rating}: {count_by_rating} отзывов")
        
        client.close()
        
        print("\n" + "=" * 60)
        print("✅ ДАННЫЕ ПРОВЕРЕНЫ И ОБНОВЛЕНЫ")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

# Создаем DAG
dag = DAG(
    'test_mongodb',
    default_args=default_args,
    description='Тестирование подключения к MongoDB и проверка данных',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'mongodb', 'diploma'],
)

# Задачи
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

test_connection_task = PythonOperator(
    task_id='test_mongodb_connection',
    python_callable=test_mongodb_connection,
    dag=dag,
)

check_data_task = PythonOperator(
    task_id='check_and_create_test_data',
    python_callable=check_and_create_test_data,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Определяем зависимости
start_task >> test_connection_task >> check_data_task >> end_task
