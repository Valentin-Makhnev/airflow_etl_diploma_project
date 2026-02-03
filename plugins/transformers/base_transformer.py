from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """Базовый класс для трансформации данных"""
    
    def __init__(self):
        self.stats = {}
        
    @abstractmethod
    def transform(self, data, **kwargs):
        """Трансформация данных"""
        pass
    
    def get_stats(self):
        """Получение статистики трансформации"""
        return self.stats
    