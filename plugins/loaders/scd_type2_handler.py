"""
SCD Type 2 Handler - адаптирован под структуру таблицы dim_customers
"""
import pandas as pd
from datetime import date, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

class SCDType2Handler:
    """Обработчик SCD Type 2 для измерений"""
    
    def __init__(self, conn_id, table_name, natural_key):
        self.conn_id = conn_id
        self.table_name = table_name
        self.natural_key = natural_key
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
    
    def process_dimension(self, new_data, effective_date=None):
        """
        Обработка измерения dim_customers с учетом структуры таблицы
        """
        if effective_date is None:
            effective_date = date.today()
        
        print(f"🔄 Обработка измерения {self.table_name}...")
        
        results = {
            'new_records': 0,
            'updated_records': 0,
            'unchanged_records': 0
        }
        
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for _, row in new_data.iterrows():
                natural_key_value = row[self.natural_key]
                
                # Проверяем существует ли текущая запись
                check_query = f"""
                SELECT first_name, last_name, email, city, is_current
                FROM {self.table_name} 
                WHERE {self.natural_key} = %s 
                AND is_current = TRUE
                """
                
                cursor.execute(check_query, (natural_key_value,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Проверяем, изменились ли данные
                    existing_first_name = existing_record[0] or ''
                    existing_last_name = existing_record[1] or ''
                    existing_email = existing_record[2] or ''
                    existing_city = existing_record[3] or ''
                    
                    # Сравниваем с новыми данными
                    new_first_name = row.get('first_name', '')
                    new_last_name = row.get('last_name', '')
                    new_email = row.get('email', '')
                    new_city = row.get('city', '')
                    
                    # Приводим к строке и убираем пробелы для сравнения
                    existing_str = f"{existing_first_name}{existing_last_name}{existing_email}{existing_city}".lower().strip()
                    new_str = f"{new_first_name}{new_last_name}{new_email}{new_city}".lower().strip()
                    
                    if existing_str != new_str:
                        # Закрываем старую версию
                        update_query = f"""
                        UPDATE {self.table_name}
                        SET is_current = FALSE,
                            expiration_date = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {self.natural_key} = %s 
                        AND is_current = TRUE
                        """
                        cursor.execute(update_query, (effective_date, natural_key_value))
                        
                        # Вставляем новую версию
                        insert_query = f"""
                        INSERT INTO {self.table_name} 
                        (customer_id, first_name, last_name, email, city, 
                         effective_date, expiration_date, is_current, source_system)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, 'postgres_source')
                        """
                        cursor.execute(insert_query, (
                            natural_key_value,
                            new_first_name,
                            new_last_name,
                            new_email,
                            new_city,
                            effective_date,
                            '9999-12-31'
                        ))
                        
                        results['updated_records'] += 1
                        print(f"📝 Обновлена запись для customer_id={natural_key_value}")
                    else:
                        results['unchanged_records'] += 1
                        print(f"⏭ Запись без изменений customer_id={natural_key_value}")
                else:
                    # Новая запись - просто вставляем
                    insert_query = f"""
                    INSERT INTO {self.table_name} 
                    (customer_id, first_name, last_name, email, city, 
                     effective_date, expiration_date, is_current, source_system)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, 'postgres_source')
                    """
                    
                    cursor.execute(insert_query, (
                        natural_key_value,
                        row.get('first_name', ''),
                        row.get('last_name', ''),
                        row.get('email', ''),
                        row.get('city', ''),
                        effective_date,
                        '9999-12-31'
                    ))
                    
                    results['new_records'] += 1
                    print(f"✅ Добавлена новая запись для customer_id={natural_key_value}")
            
            conn.commit()
            print(f"✅ Все изменения сохранены в БД")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Ошибка при обработке: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        print(f"📊 Обработка завершена: {results}")
        return results
    