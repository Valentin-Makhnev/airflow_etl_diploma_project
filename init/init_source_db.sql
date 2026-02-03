-- Таблица клиентов
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100) DEFAULT 'Russia',
    registration_date DATE DEFAULT CURRENT_DATE,
    customer_segment VARCHAR(50) DEFAULT 'Standard',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица товаров
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2),
    stock_quantity INTEGER DEFAULT 0,
    supplier_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    order_time TIME NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'Pending',
    payment_method VARCHAR(50),
    shipping_address TEXT,
    shipping_city VARCHAR(100),
    shipping_country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица деталей заказа
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица платежей
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    payment_date TIMESTAMP NOT NULL,
    payment_amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50) DEFAULT 'Completed',
    transaction_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для производительности
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_products_category ON products(category);

-- Триггер для обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Вставка тестовых данных (опционально)
INSERT INTO customers (first_name, last_name, email, phone, city, country, customer_segment) VALUES
('Иван', 'Иванов', 'ivanov@example.com', '+79161234567', 'Москва', 'Russia', 'VIP'),
('Мария', 'Петрова', 'petrova@example.com', '+79167654321', 'Санкт-Петербург', 'Russia', 'Standard'),
('Алексей', 'Сидоров', 'sidorov@example.com', '+79031234567', 'Новосибирск', 'Russia', 'Standard'),
('Елена', 'Кузнецова', 'kuznetsova@example.com', '+79151112233', 'Казань', 'Russia', 'Premium'),
('Дмитрий', 'Смирнов', 'smirnov@example.com', '+79263334455', 'Екатеринбург', 'Russia', 'Standard')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (product_name, category, subcategory, brand, unit_price, cost_price, stock_quantity) VALUES
('Ноутбук Dell XPS 13', 'Электроника', 'Ноутбуки', 'Dell', 89999.99, 75000.00, 15),
('Смартфон iPhone 15', 'Электроника', 'Смартфоны', 'Apple', 99999.99, 85000.00, 25),
('Кроссовки Nike Air Max', 'Одежда', 'Обувь', 'Nike', 7999.99, 5000.00, 50),
('Кофеварка DeLonghi', 'Бытовая техника', 'Кофеварки', 'DeLonghi', 24999.99, 20000.00, 30),
('Книга "Python для анализа данных"', 'Книги', 'Программирование', 'Питер', 1499.99, 1000.00, 100),
('Наушники Sony WH-1000XM4', 'Электроника', 'Аудио', 'Sony', 29999.99, 25000.00, 40),
('Куртка зимняя', 'Одежда', 'Верхняя одежда', 'Zara', 15999.99, 12000.00, 35),
('Игровая консоль PlayStation 5', 'Электроника', 'Игровые консоли', 'Sony', 59999.99, 50000.00, 10),
('Чайник электрический', 'Бытовая техника', 'Чайники', 'Bosch', 3999.99, 3000.00, 60),
('Сумка кожаная', 'Аксессуары', 'Сумки', 'Armani', 25999.99, 20000.00, 20)
ON CONFLICT DO NOTHING;
