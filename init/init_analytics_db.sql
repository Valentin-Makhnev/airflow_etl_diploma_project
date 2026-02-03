-- Таблица для ежедневной бизнес-аналитики
CREATE TABLE IF NOT EXISTS daily_business_analytics (
    analytics_date DATE PRIMARY KEY,
    
    -- Метрики по заказам
    total_orders INTEGER DEFAULT 0,
    new_orders INTEGER DEFAULT 0,
    cancelled_orders INTEGER DEFAULT 0,
    
    -- Финансовые метрики
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    total_cost DECIMAL(12, 2) DEFAULT 0,
    total_profit DECIMAL(12, 2) DEFAULT 0,
    profit_margin DECIMAL(5, 2) DEFAULT 0,
    
    -- Метрики по клиентам
    active_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,
    
    -- Метрики по продуктам
    top_product_id INTEGER,
    top_product_name VARCHAR(255),
    top_product_revenue DECIMAL(12, 2) DEFAULT 0,
    
    -- География
    top_city VARCHAR(100),
    orders_by_city JSONB,
    
    -- Категории
    top_category VARCHAR(100),
    revenue_by_category JSONB,
    
    -- Доставка и обслуживание
    avg_delivery_time_hours DECIMAL(5, 2),
    delivery_success_rate DECIMAL(5, 2),
    
    -- Качество сервиса
    avg_customer_rating DECIMAL(3, 2),
    complaint_count INTEGER DEFAULT 0,
    
    -- Технические поля
    data_source VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX idx_analytics_date ON daily_business_analytics(analytics_date);
CREATE INDEX idx_analytics_revenue ON daily_business_analytics(total_revenue);

-- Таблица для отслеживания качества данных
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    source_name VARCHAR(100),
    
    -- Метрики качества
    total_records INTEGER DEFAULT 0,
    valid_records INTEGER DEFAULT 0,
    invalid_records INTEGER DEFAULT 0,
    duplicate_records INTEGER DEFAULT 0,
    null_values_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    
    -- Временные метрики
    processing_time_seconds DECIMAL(10, 2),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    
    -- Статус
    status VARCHAR(50),
    error_message TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для качества данных
CREATE INDEX idx_quality_run_date ON data_quality_metrics(run_date);
CREATE INDEX idx_quality_source ON data_quality_metrics(source_name);

-- Представление для быстрого доступа к последним метрикам
CREATE OR REPLACE VIEW latest_data_quality AS
SELECT 
    source_name,
    run_date,
    total_records,
    valid_records,
    ROUND((valid_records::DECIMAL / NULLIF(total_records, 0)) * 100, 2) as quality_percentage,
    processing_time_seconds
FROM data_quality_metrics
WHERE run_date = (SELECT MAX(run_date) FROM data_quality_metrics)
ORDER BY source_name;

-- Вставка тестовых данных
INSERT INTO daily_business_analytics (
    analytics_date,
    total_orders,
    total_revenue,
    avg_order_value,
    active_customers,
    top_city,
    avg_customer_rating
) VALUES 
    (CURRENT_DATE - INTERVAL '2 days', 150, 1250000.00, 8333.33, 120, 'Москва', 4.5),
    (CURRENT_DATE - INTERVAL '1 day', 165, 1400000.00, 8484.85, 135, 'Санкт-Петербург', 4.6),
    (CURRENT_DATE, 180, 1550000.00, 8611.11, 150, 'Москва', 4.7)
ON CONFLICT (analytics_date) DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    avg_order_value = EXCLUDED.avg_order_value,
    active_customers = EXCLUDED.active_customers,
    top_city = EXCLUDED.top_city,
    avg_customer_rating = EXCLUDED.avg_customer_rating,
    updated_at = CURRENT_TIMESTAMP;