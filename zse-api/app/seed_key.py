from app.db import get_db_cursor
import hashlib

def generate_test_key():
    """Generates a test API key and inserts it into the DB"""
    test_key = "test_key_123"
    key_hash = hashlib.sha256(test_key.encode()).hexdigest()
    
    print(f"Generating test API key: {test_key}")
    
    with get_db_cursor(commit=True) as cur:
        cur.execute("""
            INSERT INTO api_keys (key_hash, key_prefix, user_email, tier, daily_limit, monthly_limit, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            On CONFLICT (key_hash) DO NOTHING;
        """, (key_hash, "test_key", "admin@localhost", "pro", 1000, 100000, True))
    
    print("Test key inserted/verified.")

if __name__ == "__main__":
    generate_test_key()
