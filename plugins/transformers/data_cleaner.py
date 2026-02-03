"""
Data Cleaner - упрощенная версия
"""
import pandas as pd
import numpy as np

class DataCleaner:
    """Очистка данных"""
    
    def __init__(self):
        self.stats = {
            'original_rows': 0,
            'cleaned_rows': 0,
            'duplicates_removed': 0,
            'nulls_filled': 0
        }
    
    def transform(self, data, **kwargs):
        """Очистка данных"""
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame(data)
            
        self.stats['original_rows'] = len(df)
        
        print("🧹 Очистка данных...")
        
        # Удаление дубликатов
        if 'primary_key' in kwargs:
            pk = kwargs['primary_key']
            before = len(df)
            df = df.drop_duplicates(subset=[pk], keep='last')
            self.stats['duplicates_removed'] = before - len(df)
        
        # Заполнение пропущенных значений
        fill_rules = kwargs.get('fill_rules', {})
        for column, value in fill_rules.items():
            if column in df.columns:
                null_count = df[column].isnull().sum()
                df[column] = df[column].fillna(value)
                self.stats['nulls_filled'] += null_count
        
        self.stats['cleaned_rows'] = len(df)
        print(f"✅ Очистка завершена. Сохранено {self.stats['cleaned_rows']} записей")
        
        return df
    
    def get_stats(self):
        return self.stats
    