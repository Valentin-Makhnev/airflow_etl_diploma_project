#!/usr/bin/env python3
import os
import time
import subprocess
import sys

def run_command(cmd, max_retries=3):
    """Выполнить команду с повторами при ошибке"""
    for attempt in range(max_retries):
        try:
            print(f"Выполнение: {cmd}")
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Успешно: {result.stdout}")
                return True
            else:
                print(f"Ошибка (попытка {attempt + 1}/{max_retries}): {result.stderr}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    
        except Exception as e:
            print(f"Исключение при выполнении команды: {e}")
            
    return False

def init_airflow():
    """Инициализация Airflow"""
    
    print("=" * 60)
    print("ИНИЦИАЛИЗАЦИЯ AIRFLOW")
    print("=" * 60)
    
    # Шаг 1: Проверка подключения к БД
    print("\n1. Проверка подключения к БД метаданных...")
    if not run_command("airflow db check"):
        print("❌ Не удалось подключиться к БД метаданных")
        sys.exit(1)
    
    # Шаг 2: Миграция БД
    print("\n2. Миграция БД...")
    if not run_command("airflow db migrate"):
        print("❌ Не удалось выполнить миграцию БД")
        sys.exit(1)
    
    # Шаг 3: Проверка существующих пользователей
    print("\n3. Проверка существующих пользователей...")
    result = subprocess.run(
        "airflow users list", 
        shell=True, 
        capture_output=True, 
        text=True
    )
    
    has_admin = False
    if result.returncode == 0:
        if "admin" in result.stdout:
            has_admin = True
            print("✅ Пользователь admin уже существует")
    
    # Шаг 4: Создание пользователя (если нужно)
    if not has_admin:
        print("\n4. Создание пользователя admin...")
        create_user_cmd = """
        airflow users create \
            --username admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com \
            --password admin
        """
        if not run_command(create_user_cmd):
            print("❌ Не удалось создать пользователя admin")
            sys.exit(1)
    
    # Шаг 5: Настройка подключений
    print("\n5. Настройка подключений...")
    try:
        from setup_connections import setup_airflow_connections
        setup_airflow_connections()
    except Exception as e:
        print(f"⚠ Ошибка при настройке подключений: {e}")
        print("Продолжаем без подключений...")
    
    print("\n" + "=" * 60)
    print("✅ AIRFLOW УСПЕШНО ИНИЦИАЛИЗИРОВАН")
    print("=" * 60)

if __name__ == "__main__":
    init_airflow()