from abc import ABC, abstractmethod

class BaseLoader(ABC):
    """Базовый класс для загрузки данных"""
    
    def __init__(self, conn_id=None):
        self.conn_id = conn_id
        self.connection = None
        
    @abstractmethod
    def connect(self):
        """Установка соединения с целевой системой"""
        pass
    
    @abstractmethod
    def load(self, data, **kwargs):
        """Загрузка данных"""
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
        