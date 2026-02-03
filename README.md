# Дипломный проект: ETL Pipeline с SCD Type 2
Проект создан в образовательных целях для дипломной работы.
## Описание
Тема: "Дашборд аналитика бизнес-процессов"
Предметная область: Интернет-магазин
Цель: Разработать ETL-pipeline на базе Apache Airflow для автоматизированного сбора, обработки и визуализации данных бизнес-процессов

┌─────────────────────────────────────────────────────────┐
│                    ИСТОЧНИКИ ДАННЫХ                     │
├──────────────┬──────────────┬───────────────────────────┤
│  PostgreSQL  │   MongoDB    │      CSV файлы            │
│   (customers,│  (feedback)  │    (доп. продукты)        │
│   products,  │              │                           │
│    orders)   │              │                           │
└──────┬───────┴──────┬───────┴────────────┬──────────────┘
       │              │                     │
       └──────────────┴─────────────────────┘
                              ↓
    ┌────────────────────────────────────────────────────┐
    │            AIRFLOW DAG (final_etl_working)         │
    │              Ежедневно в 9:00 AM                   │
    └──────────────────────┬─────────────────────────────┘
                           ↓
    ┌────────────────────────────────────────────────────┐
    │               ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА               │
    ├─────────────────────┬──────────────────────────────┤
    │ Основной поток      │ CSV поток (низкий приоритет) │
    └─────────────────────┴──────────────────────────────┘
                           ↓
    ┌────────────────────────────────────────────────────┐
    │               ЦЕЛЕВЫЕ ХРАНИЛИЩА                    │
    ├─────────────────────┬──────────────────────────────┤
    │   Data Warehouse    │    Analytics Database        │
    │    (postgres-dwh)   │    (postgres-analytics)      │
    └──────────┬──────────┴──────────────┬───────────────┘
               │                         │
               └───────────┬─────────────┘
                           ↓
                 Готово для визуализации
                 (Grafana / Superset / Metabase)

## Структура проекта
- `dags/` - DAG'и Airflow
- `init/` - SQL скрипты инициализации БД
- `plugins/` - кастомные плагины Airflow
- `scripts/` - вспомогательные скрипты

## Быстрый старт
1. Клонирование и настройка
2. Запустить: `docker-compose up -d`
3. Открыть Airflow UI: http://localhost:8080
4. Запустить DAG "final_etl_dag"

## Доступ к сервисам
Сервис	        URL	                    Учетные данные
Airflow UI	    http://localhost:8080	admin/admin
PgAdmin	        http://localhost:5050	admin@example.com/admin
Mongo Express	http://localhost:5051	mongo_user/mongo_password123

## Инициализация данных
Базы данных инициализируются автоматически при первом запуске:

PostgreSQL Source: таблицы customers, products, orders

Data Warehouse: dim_customers с SCD Type 2 структурой

Analytics DB: daily_business_analytics

MongoDB: коллекция customer_feedback

# Через Airflow UI:
# 1. Открыть http://localhost:8080
# 2. Найти DAG 'final_etl_working'
# 3. Запустить

# Или через CLI:
docker-compose exec airflow-webserver airflow dags trigger final_etl_working

📊 Описание ETL пайплайна
Фаза 1: Извлечение данных (EXTRACT)
1.1 PostgreSQL Source

# Извлекаем данные из PostgreSQL
with PostgresExtractor(conn_id='postgres_source') as extractor:
    customers_df = extractor.extract_table('customers', where_clause='1=1 LIMIT 10')
    products_df = extractor.extract_table('products', where_clause='1=1 LIMIT 10')
    orders_df = extractor.extract_table('orders', where_clause=f"order_date >= '{start_date}' LIMIT 10")

Таблицы:

- customers: 10 записей (клиенты)

- products: 10 записей (продукты)

- orders: 3 записи (последние 7 дней)

1.2 MongoDB

# Извлекаем отзывы из MongoDB
mongo_extractor = MongoExtractor(conn_id='mongodb_source')
feedback_data = mongo_extractor.extract_collection('customer_feedback', database='source_mongo_db', limit=10)

Коллекция:

 - customer_feedback: 10 документов (отзывы клиентов)

1.3 CSV файлы

# Извлекаем дополнительные данные о продуктах из CSV
csv_extractor = CSVExtractor(file_path='/opt/airflow/data/csv/csv_products.csv')
csv_df = csv_extractor.extract_csv(sep=',', encoding='utf-8', parse_dates=['created_at', 'updated_at'])

Файл:

 - csv_products.csv: 10 записей (дополнительные продукты с расширенными атрибутами)

Фаза 2: Трансформация данных (TRANSFORM)

2.1 Очистка данных

# Очистка клиентов
customers_df['city'] = customers_df['city'].fillna('Не указан')
customers_df['country'] = customers_df['country'].fillna('Россия')

# Очистка заказов
orders_df['status'] = orders_df['status'].fillna('Pending')

2.2 Валидация CSV данных

# Проверка обязательных полей
required_columns = ['product_id', 'product_name', 'category', 'unit_price']
validation = csv_extractor.validate_csv(csv_df, required_columns)

Фаза 3: Загрузка данных (LOAD)
3.1 Data Warehouse с SCD Type 2
Таблица dim_customers (SCD Type 2):

CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Логика SCD Type 2:

 - Проверка изменений в атрибутах клиента

 - Создание новой версии при изменениях

 - Закрытие старой версии (expiration_date = дата изменения - 1 день)

 - Привязка фактов к правильной версии измерения

3.2 Фактические таблицы

-- Факты отзывов
CREATE TABLE fact_feedback (
    feedback_key SERIAL PRIMARY KEY,
    feedback_id VARCHAR(50),
    feedback_text TEXT,
    customer_id INTEGER DEFAULT 0,
    product_id INTEGER DEFAULT 0,
    rating INTEGER DEFAULT 0,
    source_system VARCHAR(50) DEFAULT 'mongo_source'
);

-- CSV продукты
CREATE TABLE csv_products (
    csv_product_key SERIAL PRIMARY KEY,
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10, 2),
    stock_quantity INTEGER,
    supplier VARCHAR(100),
    country_of_origin VARCHAR(100),
    weight_kg DECIMAL(6, 2),
    dimensions VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'csv_source'
);

3.3 Аналитическая БД

-- Агрегированные метрики по дням
CREATE TABLE daily_business_analytics (
    analytics_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    active_customers INTEGER,
    top_city VARCHAR(100),
    avg_customer_rating DECIMAL(3, 2),
    data_source VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Расписание и зависимости задач
Граф выполнения DAG:

start_etl
    ├── extract_with_plugins (PostgreSQL + MongoDB)
    │   └── transform_data
    │       ├── load_to_dwh_scd_type2
    │       ├── load_feedback_to_dwh
    │       └── load_to_analytics
    │
    └── extract_csv_data (параллельно, низкий приоритет)
        └── load_csv_to_dwh
            └── validate_results
                └── end_etl

Расписание:
Основной запуск: Ежедневно в 9:00 AM (schedule_interval='0 9 * * *')

Ручной запуск: В любое время через Airflow UI

Таймаут: 15 минут на выполнение всего DAG
