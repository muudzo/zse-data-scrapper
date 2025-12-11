import os
import time
import logging
from contextlib import contextmanager
from typing import Generator
import psycopg
from psycopg.rows import dict_row

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use environment variables correctly
# Construct DATABASE_URL if not provided, or separate params
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "zse_db")

DEFAULT_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

def get_db_connection(conn_str: str = DATABASE_URL) -> psycopg.Connection:
    """Create a database connection with retries"""
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            # Usage of row_factory=dict_row enables accessing columns by name
            conn = psycopg.connect(conn_str, row_factory=dict_row, autocommit=False)
            return conn
        except psycopg.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection failed (attempt {attempt+1}/{max_retries}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error("Could not connect to database after maximum retries.")
                raise e

@contextmanager
def get_db_cursor(commit: bool = False) -> Generator[psycopg.Cursor, None, None]:
    """
    Context manager for database cursor.
    Handles connection open/close and transaction commit/rollback.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
