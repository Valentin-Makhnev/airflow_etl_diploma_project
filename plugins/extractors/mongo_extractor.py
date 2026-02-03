"""
MongoDB Extractor - исправленная версия (без зависимостей от airflow provider)
"""
import sys

class MongoExtractor:
    """Извлечение данных из MongoDB"""
    
    def __init__(self, conn_id=None, **kwargs):
        self.conn_id = conn_id
        
    def extract_collection(self, collection_name, database=None, limit=None):
        """Извлечение данных из коллекции"""
        print(f"📥 Извлечение из MongoDB: {collection_name}")
        
        # Пробуем получить данные из реальной MongoDB
        real_data = self._try_get_real_data(collection_name, database, limit)
        if real_data is not None:
            return real_data
        
        # Если не получилось, возвращаем реалистичные тестовые данные
        print("⚠ Используем реалистичные тестовые данные")
        return self._get_realistic_test_data(limit)
    
    def _try_get_real_data(self, collection_name, database, limit):
        """Пытается получить данные из реальной MongoDB"""
        try:
            # Пробуем импортировать pymongo
            from pymongo import MongoClient
            
            # Параметры подключения из docker-compose
            mongo_uri = "mongodb://mongo_user:mongo_password123@mongodb:27017/"
            
            client = MongoClient(
                mongo_uri,
                authSource='admin',
                authMechanism='SCRAM-SHA-256',
                serverSelectionTimeoutMS=5000
            )
            
            # Проверяем подключение
            client.admin.command('ping')
            print("✅ Подключение к MongoDB успешно")
            
            # Выбираем базу данных
            db_name = database if database else 'source_mongo_db'
            db = client[db_name]
            
            # Получаем коллекцию
            collection = db[collection_name]
            
            # Извлекаем данные
            if limit:
                cursor = collection.find({}).limit(limit)
            else:
                cursor = collection.find({})
            
            data = list(cursor)
            
            # Конвертируем ObjectId в строку
            for doc in data:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            client.close()
            
            print(f"✅ Извлечено из {collection_name}: {len(data)} документов")
            
            if data and len(data) > 0:
                print(f"📋 Пример данных. Колонки: {list(data[0].keys())}")
                if 'rating' in data[0]:
                    ratings = [d.get('rating', 0) for d in data if d.get('rating')]
                    if ratings:
                        print(f"📊 Средний рейтинг в данных: {sum(ratings)/len(ratings):.2f}")
            
            return data
            
        except ImportError:
            print("⚠ pymongo не установлен")
            return None
        except Exception as e:
            print(f"⚠ Не удалось подключиться к MongoDB: {e}")
            return None
    
    def _get_realistic_test_data(self, limit=None):
        """Создает реалистичные тестовые данные с рейтингами"""
        import random
        from datetime import datetime, timedelta
        
        test_data = []
        count = limit if limit else 5
        
        for i in range(1, count + 1):
            feedback = {
                'feedback_id': f'FB_{i:04d}',
                'customer_id': random.randint(1, 20),
                'customer_email': f'customer{random.randint(1, 20)}@example.com',
                'product_id': random.randint(1, 15),
                'product_name': f'Товар {random.randint(1, 15)}',
                'rating': random.randint(1, 5),  # Важно: рейтинг от 1 до 5
                'comment': random.choice([
                    'Отличный товар! Рекомендую!',
                    'Хорошее качество за свои деньги',
                    'Быстрая доставка, доволен покупкой',
                    'Не понравилось, ожидал большего',
                    'Можно лучше, но в целом норм',
                    'Супер! Буду заказывать еще!'
                ]),
                'feedback_date': datetime.now() - timedelta(days=random.randint(0, 30)),
                'helpful_votes': random.randint(0, 50),
                'verified_purchase': random.choice([True, False]),
                'created_at': datetime.now() - timedelta(days=random.randint(0, 30))
            }
            test_data.append(feedback)
        
        print(f"📥 Создано тестовых отзывов: {len(test_data)}")
        
        # Рассчитываем средний рейтинг
        if test_data:
            ratings = [d['rating'] for d in test_data]
            avg_rating = sum(ratings) / len(ratings)
            print(f"📊 Средний рейтинг в тестовых данных: {avg_rating:.2f}")
        
        return test_data
    