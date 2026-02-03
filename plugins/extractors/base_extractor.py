from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    """Базовый класс для извлечения данных"""
    
    def __init__(self, conn_id=None, **kwargs):
        self.conn_id = conn_id
        self.connection = None
        
    @abstractmethod
    def connect(self):
        """Установка соединения с источником данных"""
        pass
    
    @abstractmethod
    def extract(self, **kwargs):
        """Извлечение данных"""
        pass
    
    def close(self):
        """Закрытие соединения"""
        if self.connection:
            self.connection.close()
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        