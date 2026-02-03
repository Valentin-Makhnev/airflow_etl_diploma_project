-- Измерение дат (статическое)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,           -- Формат: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,           -- 1=Понедельник, 7=Воскресенье
    day_name VARCHAR(10) NOT NULL,
    week_number INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение времени (статическое)
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INTEGER PRIMARY KEY,           -- Формат: HHMMSS
    full_time TIME NOT NULL UNIQUE,
    hour_24 INTEGER NOT NULL,
    hour_12 INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    am_pm VARCHAR(2) NOT NULL,
    time_of_day VARCHAR(20) NOT NULL,       -- 'Утро', 'День', 'Вечер', 'Ночь'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение клиентов (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,        -- Surrogate key (автоинкремент)
    customer_id INTEGER NOT NULL,           -- Natural key (из source системы)
    
    -- Атрибуты клиента
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(201) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    registration_date DATE,
    
    -- SCD Type 2 поля
    effective_date DATE NOT NULL,           -- Дата начала действия версии
    expiration_date DATE DEFAULT '9999-12-31',  -- Дата окончания действия
    is_current BOOLEAN DEFAULT TRUE,        -- Флаг текущей версии
    
    -- Технические поля
    source_system VARCHAR(50) DEFAULT 'postgres_source',
    load_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение продуктов (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,         -- Surrogate key
    product_id INTEGER NOT NULL,            -- Natural key
    
    -- Атрибуты продукта
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    cost_price DECIMAL(10, 2),
    stock_quantity INTEGER,
    
    -- SCD Type 2 поля
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Технические поля
    source_system VARCHAR(50) DEFAULT 'postgres_source',
    load_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение статусов заказов
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_key SERIAL PRIMARY KEY,
    status_code VARCHAR(20) NOT NULL UNIQUE,
    status_name VARCHAR(50) NOT NULL,
    status_category VARCHAR(50),
    description TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========== ТАБЛИЦЫ ФАКТОВ ==========

-- Факты заказов
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_order_id BIGSERIAL PRIMARY KEY,
    
    -- Surrogate keys измерений
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    status_key INTEGER REFERENCES dim_order_status(status_key),
    
    -- Natural keys (для отладки)
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    
    -- Дегенерированные измерения
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_city VARCHAR(100),
    
    -- Меры (все числовые, аддитивные)
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    cost_amount DECIMAL(12, 2),
    profit_amount DECIMAL(12, 2),
    
    -- Технические поля
    source_system VARCHAR(50) DEFAULT 'postgres_source',
    load_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ограничения
    CHECK (quantity > 0),
    CHECK (unit_price >= 0),
    CHECK (total_amount >= 0)
);

-- Факты платежей
CREATE TABLE IF NOT EXISTS fact_payments (
    fact_payment_id BIGSERIAL PRIMARY KEY,
    
    -- Surrogate keys
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Natural keys
    payment_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    
    -- Меры
    payment_amount DECIMAL(12, 2) NOT NULL,
    
    -- Дегенерированные измерения
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    
    -- Технические поля
    source_system VARCHAR(50) DEFAULT 'postgres_source',
    load_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========== ИНДЕКСЫ ==========

-- Для dim_customers
CREATE INDEX idx_dim_customers_customer_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_customers_current ON dim_customers(customer_id, is_current);
CREATE INDEX idx_dim_customers_dates ON dim_customers(effective_date, expiration_date);
CREATE INDEX idx_dim_customers_city ON dim_customers(city);

-- Для dim_products
CREATE INDEX idx_dim_products_product_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_current ON dim_products(product_id, is_current);
CREATE INDEX idx_dim_products_category ON dim_products(category);

-- Для fact_orders
CREATE INDEX idx_fact_orders_date_key ON fact_orders(date_key);
CREATE INDEX idx_fact_orders_customer_key ON fact_orders(customer_key);
CREATE INDEX idx_fact_orders_product_key ON fact_orders(product_key);
CREATE INDEX idx_fact_orders_order_id ON fact_orders(order_id);

-- Для fact_payments
CREATE INDEX idx_fact_payments_date_key ON fact_payments(date_key);
CREATE INDEX idx_fact_payments_customer_key ON fact_payments(customer_key);

-- ========== ВСТАВКА СТАТИЧЕСКИХ ДАННЫХ ==========

-- Заполнение dim_order_status
INSERT INTO dim_order_status (status_code, status_name, status_category, description) VALUES
('PENDING', 'В обработке', 'Активный', 'Заказ создан, ожидает обработки'),
('PROCESSING', 'Обрабатывается', 'Активный', 'Заказ находится в обработке'),
('SHIPPED', 'Отправлен', 'Активный', 'Заказ отправлен клиенту'),
('DELIVERED', 'Доставлен', 'Завершен', 'Заказ успешно доставлен'),
('CANCELLED', 'Отменен', 'Отменен', 'Заказ был отменен'),
('REFUNDED', 'Возвращен', 'Возврат', 'Заказ был возвращен')
ON CONFLICT (status_code) DO NOTHING;

-- Функция для заполнения dim_date (заполняет на 10 лет вперед и 2 года назад)
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE DEFAULT '2023-01-01', end_date DATE DEFAULT '2030-12-31')
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            full_date,
            day_of_month,
            day_of_week,
            day_name,
            week_number,
            month_number,
            month_name,
            quarter,
            year,
            is_weekend
        ) VALUES (
            EXTRACT(YEAR FROM current_date) * 10000 + 
            EXTRACT(MONTH FROM current_date) * 100 + 
            EXTRACT(DAY FROM current_date),
            current_date,
            EXTRACT(DAY FROM current_date),
            EXTRACT(ISODOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            EXTRACT(WEEK FROM current_date),
            EXTRACT(MONTH FROM current_date),
            TO_CHAR(current_date, 'Month'),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(YEAR FROM current_date),
            EXTRACT(ISODOW FROM current_date) IN (6, 7)
        ) ON CONFLICT (date_key) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Функция для заполнения dim_time
CREATE OR REPLACE FUNCTION populate_dim_time()
RETURNS VOID AS $$
DECLARE
    current_time TIME := '00:00:00';
    time_key_val INTEGER;
    time_of_day_val VARCHAR(20);
BEGIN
    FOR i IN 0..86399 LOOP
        time_key_val := 
            EXTRACT(HOUR FROM current_time) * 10000 + 
            EXTRACT(MINUTE FROM current_time) * 100 + 
            EXTRACT(SECOND FROM current_time);
        
        -- Определение времени суток
        CASE 
            WHEN EXTRACT(HOUR FROM current_time) BETWEEN 6 AND 11 THEN time_of_day_val := 'Утро';
            WHEN EXTRACT(HOUR FROM current_time) BETWEEN 12 AND 17 THEN time_of_day_val := 'День';
            WHEN EXTRACT(HOUR FROM current_time) BETWEEN 18 AND 23 THEN time_of_day_val := 'Вечер';
            ELSE time_of_day_val := 'Ночь';
        END CASE;
        
        INSERT INTO dim_time (
            time_key,
            full_time,
            hour_24,
            hour_12,
            minute,
            second,
            am_pm,
            time_of_day
        ) VALUES (
            time_key_val,
            current_time,
            EXTRACT(HOUR FROM current_time),
            CASE WHEN EXTRACT(HOUR FROM current_time) > 12 
                 THEN EXTRACT(HOUR FROM current_time) - 12 
                 ELSE EXTRACT(HOUR FROM current_time) END,
            EXTRACT(MINUTE FROM current_time),
            EXTRACT(SECOND FROM current_time),
            CASE WHEN EXTRACT(HOUR FROM current_time) >= 12 THEN 'PM' ELSE 'AM' END,
            time_of_day_val
        ) ON CONFLICT (time_key) DO NOTHING;
        
        current_time := current_time + INTERVAL '1 second';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Вызов функций заполнения (при первом запуске)
-- SELECT populate_dim_date('2023-01-01', '2026-12-31');
-- SELECT populate_dim_time();

-- ========== ПРЕДСТАВЛЕНИЯ ДЛЯ АНАЛИТИКИ ==========

-- Представление для текущих версий клиентов
CREATE OR REPLACE VIEW v_current_customers AS
SELECT 
    customer_key,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    city,
    country,
    customer_segment,
    effective_date,
    expiration_date
FROM dim_customers
WHERE is_current = TRUE;

-- Представление для анализа изменений клиентов
CREATE OR REPLACE VIEW v_customer_history AS
SELECT 
    customer_id,
    first_name,
    last_name,
    city,
    country,
    customer_segment,
    effective_date,
    expiration_date,
    CASE 
        WHEN is_current = TRUE THEN 'Текущая'
        ELSE 'Историческая'
    END as version_status,
    DATEDIFF(DAY, effective_date, expiration_date) as days_active
FROM dim_customers
ORDER BY customer_id, effective_date;

-- Представление для ежедневных продаж
CREATE OR REPLACE VIEW v_daily_sales AS
SELECT 
    d.full_date,
    COUNT(DISTINCT fo.order_id) as total_orders,
    COUNT(DISTINCT fo.customer_id) as unique_customers,
    SUM(fo.quantity) as total_items,
    SUM(fo.total_amount) as total_revenue,
    AVG(fo.total_amount) as avg_order_value
FROM fact_orders fo
JOIN dim_date d ON fo.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date DESC;
