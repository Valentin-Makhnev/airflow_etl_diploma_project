# /opt/airflow/scripts/test_extract.py
import sys
import os

# Добавляем путь к плагинам
sys.path.insert(0, '/opt/airflow/plugins')

from extractors.postgres_extractor import PostgresExtractor
from extractors.mongo_extractor import MongoExtractor

print("Testing PostgreSQL extractor...")
try:
    with PostgresExtractor(conn_id='postgres_source') as extractor:
        customers = extractor.extract_table('customers', where_clause='1=1 LIMIT 5')
        print(f"✅ PostgreSQL extracted {len(customers)} customers")
        if len(customers) > 0:
            print(customers.head())
except Exception as e:
    print(f"❌ PostgreSQL error: {e}")
    import traceback
    traceback.print_exc()

print("\nTesting MongoDB extractor...")
try:
    mongo = MongoExtractor(conn_id='mongodb_source')
    feedback = mongo.extract_collection('customer_feedback', database='source_mongo_db', limit=5)
    print(f"✅ MongoDB extracted {len(feedback)} feedback records")
    if feedback:
        for i, doc in enumerate(feedback[:3]):
            print(f"  Document {i}: {doc}")
except Exception as e:
    print(f"❌ MongoDB error: {e}")
    import traceback
    traceback.print_exc()
    