# scripts/setup_connections.py - исправленная версия
import os
import json
from airflow.models import Connection
from airflow import settings
from sqlalchemy.orm import Session
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_airflow_connections():
    """Настройка подключений в Airflow"""
    
    connections = [
        {
            'conn_id': 'postgres_source',
            'conn_type': 'postgres',
            'host': os.getenv('POSTGRES_SOURCE_HOST', 'postgres-source'),
            'port': int(os.getenv('POSTGRES_SOURCE_PORT', '5432')),
            'login': os.getenv('POSTGRES_SOURCE_USER', 'source_user'),
            'password': os.getenv('POSTGRES_SOURCE_PASSWORD', 'source_password123'),
            'schema': os.getenv('POSTGRES_SOURCE_DB', 'source_db'),
            'description': 'Основная БД интернет-магазина'
        },
        {
            'conn_id': 'postgres_analytics',
            'conn_type': 'postgres',
            'host': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres-analytics'),
            'port': int(os.getenv('POSTGRES_ANALYTICS_PORT', '5432')),
            'login': os.getenv('POSTGRES_ANALYTICS_USER', 'analytics_user'),
            'password': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'analytics_password123'),
            'schema': os.getenv('POSTGRES_ANALYTICS_DB', 'analytics_db'),
            'description': 'Аналитическая БД для агрегированных данных'
        },
        {
            'conn_id': 'postgres_dwh',
            'conn_type': 'postgres',
            'host': os.getenv('POSTGRES_DWH_HOST', 'postgres-dwh'),
            'port': int(os.getenv('POSTGRES_DWH_PORT', '5432')),
            'login': os.getenv('POSTGRES_DWH_USER', 'dwh_user'),
            'password': os.getenv('POSTGRES_DWH_PASSWORD', 'dwh_password123'),
            'schema': os.getenv('POSTGRES_DWH_DB', 'dwh_db'),
            'description': 'Data Warehouse с SCD Type 2'
        },
        {
            'conn_id': 'mongodb_source',
            'conn_type': 'mongodb',  # ИСПРАВЛЕНО: mongodb вместо mongo
            'host': os.getenv('MONGO_HOST', 'mongodb'),
            'port': int(os.getenv('MONGO_PORT', '27017')),
            'login': os.getenv('MONGO_USER', 'mongo_user'),
            'password': os.getenv('MONGO_PASSWORD', 'mongo_password123'),
            # Удален 'schema' для MongoDB
            'extra': json.dumps({
                'database': os.getenv('MONGO_DB', 'source_mongo_db'),
                'authSource': 'admin',
                'authMechanism': 'SCRAM-SHA-256'
            }),
            'description': 'MongoDB для отзывов и логов'
        }
    ]
    
    try:
        # Получаем сессию из настроек Airflow
        session = Session(settings.engine)
        
        for conn_info in connections:
            conn_id = conn_info['conn_id']
            
            # Проверяем существует ли уже подключение
            existing = session.query(Connection).filter_by(conn_id=conn_id).first()
            
            if existing:
                logger.info(f"Обновляем подключение: {conn_id}")
                # Удаляем старое
                session.delete(existing)
                session.commit()
            
            # Создаем новое подключение
            logger.info(f"Создаем подключение: {conn_id}")
            conn = Connection(**conn_info)
            session.add(conn)
        
        session.commit()
        session.close()
        logger.info("✅ Все подключения настроены успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при настройке подключений: {e}")
        raise

if __name__ == "__main__":
    setup_airflow_connections()

    