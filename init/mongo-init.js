// Создаем базу данных для исходных данных
db = db.getSiblingDB('source_mongo_db');

// Создаем пользователя для подключения
db.createUser({
    user: 'mongo_user',
    pwd: 'mongo_password123',
    roles: [
        {
            role: 'readWrite',
            db: 'source_mongo_db'
        }
    ]
});

// Создаем коллекции и индексы
db.createCollection('customer_feedback');
db.createCollection('product_reviews');
db.createCollection('clickstream_logs');
db.createCollection('user_sessions');

// Создаем индексы для быстрого поиска
db.customer_feedback.createIndex({ "customer_id": 1 });
db.customer_feedback.createIndex({ "product_id": 1 });
db.customer_feedback.createIndex({ "feedback_date": -1 });
db.customer_feedback.createIndex({ "rating": 1 });

db.product_reviews.createIndex({ "product_id": 1 });
db.product_reviews.createIndex({ "review_date": -1 });
db.product_reviews.createIndex({ "helpful_votes": -1 });

db.clickstream_logs.createIndex({ "session_id": 1 });
db.clickstream_logs.createIndex({ "timestamp": -1 });
db.clickstream_logs.createIndex({ "user_id": 1 });

db.user_sessions.createIndex({ "session_id": 1 }, { unique: true });
db.user_sessions.createIndex({ "user_id": 1 });
db.user_sessions.createIndex({ "start_time": -1 });

// Вставляем тестовые данные для отзывов
db.customer_feedback.insertMany([
    {
        feedback_id: "FB001",
        customer_id: 1,
        customer_email: "ivanov@example.com",
        product_id: 1,
        product_name: "Ноутбук Dell XPS 13",
        rating: 5,
        comment: "Отличный ноутбук, быстрый и легкий. Батарея держит долго!",
        feedback_date: new Date("2024-01-15T10:30:00Z"),
        helpful_votes: 12,
        verified_purchase: true,
        sentiment_score: 0.9,
        created_at: new Date()
    },
    {
        feedback_id: "FB002",
        customer_id: 2,
        customer_email: "petrova@example.com",
        product_id: 2,
        product_name: "Смартфон iPhone 15",
        rating: 4,
        comment: "Хороший телефон, но цена завышена. Камера отличная.",
        feedback_date: new Date("2024-01-16T14:45:00Z"),
        helpful_votes: 8,
        verified_purchase: true,
        sentiment_score: 0.7,
        created_at: new Date()
    },
    {
        feedback_id: "FB003",
        customer_id: 3,
        customer_email: "sidorov@example.com",
        product_id: 3,
        product_name: "Кроссовки Nike Air Max",
        rating: 3,
        comment: "Удобные, но быстро изнашиваются. Через месяц появились потертости.",
        feedback_date: new Date("2024-01-17T09:15:00Z"),
        helpful_votes: 5,
        verified_purchase: true,
        sentiment_score: 0.4,
        created_at: new Date()
    },
    {
        feedback_id: "FB004",
        customer_id: 4,
        customer_email: "kuznetsova@example.com",
        product_id: 1,
        product_name: "Ноутбук Dell XPS 13",
        rating: 5,
        comment: "Лучшая покупка за последние годы! Работаю с графикой - все летает.",
        feedback_date: new Date("2024-01-18T16:20:00Z"),
        helpful_votes: 25,
        verified_purchase: true,
        sentiment_score: 0.95,
        created_at: new Date()
    },
    {
        feedback_id: "FB005",
        customer_id: 5,
        customer_email: "smirnov@example.com",
        product_id: 5,
        product_name: "Книга 'Python для анализа данных'",
        rating: 4,
        comment: "Хорошая книга для начинающих, много практических примеров.",
        feedback_date: new Date("2024-01-19T11:10:00Z"),
        helpful_votes: 3,
        verified_purchase: true,
        sentiment_score: 0.8,
        created_at: new Date()
    }
]);

// Вставляем тестовые данные для сессий пользователей
db.user_sessions.insertMany([
    {
        session_id: "SESS001",
        user_id: 1,
        email: "ivanov@example.com",
        start_time: new Date("2024-01-15T10:00:00Z"),
        end_time: new Date("2024-01-15T10:45:00Z"),
        device_type: "desktop",
        browser: "Chrome",
        os: "Windows 11",
        country: "Russia",
        city: "Москва",
        pages_visited: 12,
        session_duration_seconds: 2700,
        conversions: 1,
        created_at: new Date()
    },
    {
        session_id: "SESS002",
        user_id: 2,
        email: "petrova@example.com",
        start_time: new Date("2024-01-16T14:30:00Z"),
        end_time: new Date("2024-01-16T15:15:00Z"),
        device_type: "mobile",
        browser: "Safari",
        os: "iOS",
        country: "Russia",
        city: "Санкт-Петербург",
        pages_visited: 8,
        session_duration_seconds: 1800,
        conversions: 0,
        created_at: new Date()
    }
]);

print("MongoDB инициализирован успешно!");
print("Созданы коллекции: customer_feedback, product_reviews, clickstream_logs, user_sessions");
print("Вставлены тестовые данные для отзывов и сессий");
