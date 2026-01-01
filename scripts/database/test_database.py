# ====================================================================
# DATABASE CONNECTION
# DNA Gene Mapping Project
# Author: Sharique Mohammad
# Date: 30 December 2025
# ====================================================================
# FILE 7: scripts/database/test_database.py
# Purpose: Quick database connection test
# ====================================================================

"""
Test PostgreSQL Database Connection
Quick test script to verify database is accessible.
"""

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def test_connection():
    """Test database connection."""
    try:
        print("\n" + "="*70)
        print("TESTING POSTGRESQL CONNECTION")
        print("="*70)
        
        # Connection parameters
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DATABASE', 'genome_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        print(f"\nConnecting to:")
        print(f"  Host: {conn_params['host']}")
        print(f"  Port: {conn_params['port']}")
        print(f"  Database: {conn_params['database']}")
        print(f"  User: {conn_params['user']}")
        
        # Connect
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        print(f"\n Connection successful!")
        print(f"\nPostgreSQL version:")
        print(f"  {version}")
        
        # Check schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('bronze', 'silver', 'gold')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        
        if schemas:
            print(f"\n Schemas found:")
            for schema in schemas:
                print(f"  - {schema[0]}")
        else:
            print(f"\n Schemas not found. Run schema creation scripts first.")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*70)
        print(" DATABASE TEST PASSED!")
        print("="*70)
        
    except Exception as e:
        print(f"\n Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check PostgreSQL is running")
        print("  2. Verify .env file has correct credentials")
        print("  3. Ensure database 'genome_db' exists")
        print("="*70)


if __name__ == "__main__":
    test_connection()
