#!/usr/bin/env python3
"""
Test PostgreSQL Connection
Quick script to verify database connection works
"""

import psycopg
import os

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Build connection string
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "zse_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print("Testing PostgreSQL connection...")
print(f"Connection string: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print()

try:
    conn = psycopg.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Test query
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    
    print("✓ Connection successful!")
    print(f"PostgreSQL version: {version[:50]}...")
    print()
    
    # Check tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
    """)
    tables = cursor.fetchall()
    
    print(f"Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    conn.close()
    print()
    print("✓ Database is ready to use!")
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    print()
    print("Troubleshooting:")
    print("1. Check if PostgreSQL is running: brew services list")
    print("2. Run setup script: ./setup_local_postgres.sh")
    print("3. Verify .env file has correct values")
