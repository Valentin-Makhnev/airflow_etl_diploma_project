"""
PostgreSQL Extractor - исправленная версия
"""
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresExtractor:
    """Извлечение данных из PostgreSQL"""
    
    def __init__(self, conn_id=None, **kwargs):
        self.conn_id = conn_id
        self.connection = None
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            
    def extract_table(self, table_name, columns='*', where_clause=''):
        """Извлечение данных из таблицы"""
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        query = f"SELECT {columns} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        print(f"📥 Извлечение из {table_name}")
        df = hook.get_pandas_df(query)
        print(f"✅ Извлечено {len(df)} записей из {table_name}")
        return df
    