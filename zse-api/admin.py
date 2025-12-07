"""
ZSE API - Admin Key Management Script
Generate and manage API keys
"""

import psycopg2
import hashlib
import secrets
import os
import sys
from datetime import datetime

# Try to load .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Use defaults matching our docker-compose setup if env vars missing
DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost/zse_db"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

class APIKeyManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    def generate_key(self) -> str:
        """Generate a secure random API key"""
        return f"zse_{secrets.token_urlsafe(32)}"
    
    def hash_key(self, api_key: str) -> str:
        """Hash API key for storage"""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    def create_api_key(self, email: str, tier: str = 'free'):
        """Create a new API key"""
        # Define tier limits
        tier_limits = {
            'free': {'daily': 100, 'monthly': 5000},
            'pro': {'daily': 1000, 'monthly': 50000},
            'enterprise': {'daily': 10000, 'monthly': 1000000}
        }
        
        if tier not in tier_limits:
            print(f"Error: Invalid tier '{tier}'. Choose from: free, pro, enterprise")
            return None
        
        limits = tier_limits[tier]
        
        # Generate key
        api_key = self.generate_key()
        key_hash = self.hash_key(api_key)
        key_prefix = api_key[:8]
        
        # Store in database
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO api_keys 
                    (key_hash, key_prefix, user_email, tier, daily_limit, monthly_limit)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                key_hash,
                key_prefix,
                email,
                tier,
                limits['daily'],
                limits['monthly']
            ))
            
            key_id = cursor.fetchone()[0]
            conn.commit()
            
            print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 API KEY CREATED SUCCESSFULLY                  ║
╠══════════════════════════════════════════════════════════════╣
║ Key ID:      {key_id:<50} ║
║ Email:       {email:<50} ║
║ Tier:        {tier:<50} ║
║ Daily Limit: {limits['daily']:<50} ║
║ Monthly:     {limits['monthly']:<50} ║
╠══════════════════════════════════════════════════════════════╣
║ API KEY (SAVE THIS - IT WON'T BE SHOWN AGAIN):               ║
║ {api_key:<60} ║
╚══════════════════════════════════════════════════════════════╝

⚠️  IMPORTANT: Store this key securely. It cannot be retrieved later.

Example usage:
  curl -H "X-API-Key: {api_key}" \\
       http://localhost:8000/api/v1/securities
            """)
            
            conn.close()
            return api_key
            
        except Exception as e:
            print(f"Error creating API key: {e}")
            return None
    
    def list_keys(self):
        """List all API keys"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    id,
                    key_prefix,
                    user_email,
                    tier,
                    requests_today,
                    daily_limit,
                    requests_month,
                    monthly_limit,
                    is_active,
                    created_at,
                    last_used_at
                FROM api_keys
                ORDER BY created_at DESC
            """)
            
            keys = cursor.fetchall()
            
            if not keys:
                print("No API keys found.")
                return
            
            print(f"\n{'ID':<5} {'Prefix':<12} {'Email':<30} {'Tier':<12} {'Today':<10} {'Active':<8} {'Last Used':<20}")
            print("=" * 120)
            
            for key in keys:
                key_id, prefix, email, tier, req_today, daily_lim, req_month, monthly_lim, active, created, last_used = key
                
                usage_today = f"{req_today}/{daily_lim}"
                active_str = "✓" if active else "✗"
                last_used_str = last_used.strftime("%Y-%m-%d %H:%M") if last_used else "Never"
                
                print(f"{key_id:<5} {prefix:<12} {email:<30} {tier:<12} {usage_today:<10} {active_str:<8} {last_used_str:<20}")
            
            conn.close()
            
        except Exception as e:
            print(f"Error listing keys: {e}")
    
    def deactivate_key(self, key_id: int):
        """Deactivate an API key"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE api_keys 
                SET is_active = false
                WHERE id = %s
                RETURNING user_email
            """, (key_id,))
            
            result = cursor.fetchone()
            
            if result:
                print(f"✓ API key #{key_id} for {result[0]} has been deactivated.")
                conn.commit()
            else:
                print(f"✗ API key #{key_id} not found.")
            
            conn.close()
            
        except Exception as e:
            print(f"Error deactivating key: {e}")
    
    def reactivate_key(self, key_id: int):
        """Reactivate an API key"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE api_keys 
                SET is_active = true
                WHERE id = %s
                RETURNING user_email
            """, (key_id,))
            
            result = cursor.fetchone()
            
            if result:
                print(f"✓ API key #{key_id} for {result[0]} has been reactivated.")
                conn.commit()
            else:
                print(f"✗ API key #{key_id} not found.")
            
            conn.close()
            
        except Exception as e:
            print(f"Error reactivating key: {e}")
    
    def reset_daily_counters(self):
        """Reset daily request counters (run this daily via cron)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("UPDATE api_keys SET requests_today = 0")
            affected = cursor.rowcount
            conn.commit()
            
            print(f"✓ Reset daily counters for {affected} API keys.")
            
            conn.close()
            
        except Exception as e:
            print(f"Error resetting counters: {e}")
    
    def reset_monthly_counters(self):
        """Reset monthly request counters (run this monthly via cron)"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("UPDATE api_keys SET requests_month = 0")
            affected = cursor.rowcount
            conn.commit()
            
            print(f"✓ Reset monthly counters for {affected} API keys.")
            
            conn.close()
            
        except Exception as e:
            print(f"Error resetting counters: {e}")
    
    def get_usage_stats(self):
        """Get API usage statistics"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Overall stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_keys,
                    COUNT(CASE WHEN is_active THEN 1 END) as active_keys,
                    SUM(requests_today) as total_requests_today,
                    SUM(requests_month) as total_requests_month
                FROM api_keys
            """)
            
            stats = cursor.fetchone()
            
            print(f"""
╔══════════════════════════════════════════════════════════════╗
║                     API USAGE STATISTICS                      ║
╠══════════════════════════════════════════════════════════════╣
║ Total API Keys:        {stats[0]:<40} ║
║ Active Keys:           {stats[1]:<40} ║
║ Requests Today:        {stats[2] or 0:<40} ║
║ Requests This Month:   {stats[3] or 0:<40} ║
╚══════════════════════════════════════════════════════════════╝
            """)
            
            # Top users today
            cursor.execute("""
                SELECT user_email, requests_today
                FROM api_keys
                WHERE requests_today > 0
                ORDER BY requests_today DESC
                LIMIT 5
            """)
            
            top_users = cursor.fetchall()
            
            if top_users:
                print("\nTop Users Today:")
                print("-" * 60)
                for email, requests in top_users:
                    print(f"  {email:<40} {requests:>8} requests")
            
            conn.close()
            
        except Exception as e:
            print(f"Error getting stats: {e}")


def main():
    """CLI interface"""
    
    manager = APIKeyManager(DATABASE_URL)
    
    if len(sys.argv) < 2:
        print("""
ZSE API Key Manager

Usage:
  python admin.py create <email> [tier]     Create new API key (tier: free/pro/enterprise)
  python admin.py list                       List all API keys
  python admin.py deactivate <key_id>        Deactivate an API key
  python admin.py reactivate <key_id>        Reactivate an API key
  python admin.py stats                      Show usage statistics
  python admin.py reset-daily                Reset daily counters
  python admin.py reset-monthly              Reset monthly counters

Examples:
  python admin.py create john@example.com free
  python admin.py create corp@bigco.com enterprise
  python admin.py list
  python admin.py deactivate 5
        """)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'create':
        if len(sys.argv) < 3:
            print("Error: Email required")
            sys.exit(1)
        email = sys.argv[2]
        tier = sys.argv[3] if len(sys.argv) > 3 else 'free'
        manager.create_api_key(email, tier)
    
    elif command == 'list':
        manager.list_keys()
    
    elif command == 'deactivate':
        if len(sys.argv) < 3:
            print("Error: Key ID required")
            sys.exit(1)
        key_id = int(sys.argv[2])
        manager.deactivate_key(key_id)
    
    elif command == 'reactivate':
        if len(sys.argv) < 3:
            print("Error: Key ID required")
            sys.exit(1)
        key_id = int(sys.argv[2])
        manager.reactivate_key(key_id)
    
    elif command == 'stats':
        manager.get_usage_stats()
    
    elif command == 'reset-daily':
        manager.reset_daily_counters()
    
    elif command == 'reset-monthly':
        manager.reset_monthly_counters()
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
