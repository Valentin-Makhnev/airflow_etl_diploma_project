"""
Data Normalizer - упрощенная версия
"""
import pandas as pd
from datetime import datetime

class DataNormalizer:
    """Нормализация данных"""
    
    def __init__(self):
        self.stats = {
            'dates_normalized': 0,
            'strings_normalized': 0,
            'types_converted': 0
        }
    
    def transform(self, data, **kwargs):
        """Нормализация данных"""
        df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        
        print("📐 Нормализация данных...")
        
        # Нормализация дат
        date_columns = kwargs.get('date_columns', [])
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    self.stats['dates_normalized'] += df[col].notna().sum()
                except:
                    pass
        
        # Нормализация строк
        string_columns = kwargs.get('string_columns', [])
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                self.stats['strings_normalized'] += df[col].notna().sum()
        
        print(f"✅ Нормализация завершена")
        return df
    
    def get_stats(self):
        return self.stats
    