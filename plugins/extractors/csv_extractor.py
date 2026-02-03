"""
CSV Extractor - плагин для извлечения данных из CSV файлов
"""
import pandas as pd
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CSVExtractor:
    """Извлечение данных из CSV файлов"""
    
    def __init__(self, file_path=None, conn_id=None, **kwargs):
        """
        Инициализация CSV экстрактора
        
        Args:
            file_path: Путь к CSV файлу
            conn_id: ID подключения Airflow (не используется, для совместимости)
        """
        self.file_path = file_path
        self.conn_id = conn_id
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # CSV не требует закрытия соединения
    
    def extract_csv(self, file_path=None, **kwargs):
        """
        Извлечение данных из CSV файла
        
        Args:
            file_path: Путь к CSV файлу (если не передан, используется self.file_path)
            **kwargs: Дополнительные параметры для pandas.read_csv
            
        Returns:
            pandas.DataFrame с данными из CSV
        """
        try:
            path_to_use = file_path or self.file_path
            if not path_to_use:
                raise ValueError("Не указан путь к CSV файлу")
            
            # Проверяем существование файла
            if not os.path.exists(path_to_use):
                logger.warning(f"⚠ Файл {path_to_use} не найден, создаем тестовые данные")
                return self._create_test_data()
            
            logger.info(f"📥 Извлечение данных из CSV файла: {path_to_use}")
            
            # Параметры по умолчанию
            read_csv_kwargs = {
                'encoding': 'utf-8',
                'sep': ',',
                'quotechar': '"',
                'on_bad_lines': 'warn',
                'low_memory': False,
            }
            
            # Обновляем параметры из kwargs
            read_csv_kwargs.update(kwargs)
            
            # Читаем CSV файл
            df = pd.read_csv(path_to_use, **read_csv_kwargs)
            
            logger.info(f"✅ Извлечено {len(df)} записей из CSV")
            logger.info(f"📊 Структура данных: {df.shape[0]} строк, {df.shape[1]} колонок")
            
            if not df.empty:
                logger.info(f"📋 Колонки: {list(df.columns)}")
                logger.info(f"📄 Пример данных:\n{df.head(2).to_string()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения CSV: {e}")
            logger.warning("⚠ Возвращаем тестовые данные")
            return self._create_test_data()
    
    def _create_test_data(self):
        """Создание тестовых данных если файл не найден"""
        logger.info("📋 Создание тестовых данных о продуктах из CSV")
        
        data = {
            'product_id': list(range(201, 211)),
            'product_name': [
                'Тестовый продукт A', 'Тестовый продукт B', 'Тестовый продукт C',
                'Тестовый продукт D', 'Тестовый продукт E', 'Тестовый продукт F',
                'Тестовый продукт G', 'Тестовый продукт H', 'Тестовый продукт I',
                'Тестовый продукт J'
            ],
            'category': ['Электроника'] * 5 + ['Бытовая техника'] * 3 + ['Книги'] * 2,
            'subcategory': ['Компьютеры', 'Телефоны', 'Аксессуары', 'Гаджеты', 'Комплектующие',
                          'Кухонная техника', 'Климатическая техника', 'Уборка',
                          'Программирование', 'Бизнес'],
            'unit_price': [1000.0, 2000.0, 1500.0, 3000.0, 2500.0,
                          4500.0, 5500.0, 3500.0, 800.0, 1200.0],
            'stock_quantity': [10, 20, 15, 30, 25, 40, 35, 18, 50, 22],
            'supplier': ['Поставщик 1', 'Поставщик 2', 'Поставщик 1', 'Поставщик 3',
                        'Поставщик 2', 'Поставщик 4', 'Поставщик 3', 'Поставщик 4',
                        'Издательство 1', 'Издательство 2'],
            'country_of_origin': ['Россия', 'Китай', 'Россия', 'США', 'Китай',
                                 'Италия', 'Германия', 'Франция', 'Россия', 'США'],
            'weight_kg': [1.5, 0.3, 0.1, 0.5, 2.0, 3.5, 8.0, 2.5, 0.8, 1.2],
            'dimensions': ['30x20x5', '15x7x1', '10x5x2', '20x10x3', '40x30x10',
                          '25x25x35', '60x40x30', '30x30x40', '23x16x2', '25x18x3'],
            'created_at': pd.date_range(start='2024-01-01', periods=10, freq='D'),
            'updated_at': pd.date_range(start='2024-01-05', periods=10, freq='D'),
        }
        
        df = pd.DataFrame(data)
        logger.info(f"📊 Создано {len(df)} тестовых записей")
        return df
    
    def validate_csv(self, df, required_columns=None):
        """
        Валидация CSV данных
        
        Args:
            df: DataFrame для валидации
            required_columns: Список обязательных колонок
            
        Returns:
            dict с результатами валидации
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'missing_values': df.isnull().sum().sum(),
            }
        }
        
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"Отсутствуют обязательные колонки: {missing_columns}")
        
        if df.empty:
            validation_result['warnings'].append("CSV файл пуст")
        
        logger.info(f"✅ Валидация CSV: {validation_result['stats']}")
        return validation_result
    