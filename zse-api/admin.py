"""
ZSE API - Admin Key Management Script
Generate and manage API keys
"""

import hashlib
import secrets
import sys
from datetime import datetime
from repository import ApiKeyRepository

class APIKeyManager:
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
        
        try:
            key_id = ApiKeyRepository.create(key_hash, key_prefix, email, tier, limits)
            
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
            return api_key
            
        except Exception as e:
            print(f"Error creating API key: {e}")
            return None
    
    def list_keys(self):
        """List all API keys"""
        try:
            keys = ApiKeyRepository.list_all()
            
            if not keys:
                print("No API keys found.")
                return
            
            print(f"\n{'ID':<5} {'Prefix':<12} {'Email':<30} {'Tier':<12} {'Today':<10} {'Active':<8} {'Last Used':<20}")
            print("=" * 120)
            
            for key in keys:
                # key is a DictRow, so access by key
                # It might be a dict or tuple depending on how psycopg returns it.
                # Our repository uses RealDictRow via dict_row factory, so it returns dicts.
                
                key_id = key['id']
                prefix = key['key_prefix']
                email = key['user_email']
                tier = key['tier']
                req_today = key['requests_today']
                daily_lim = key['daily_limit']
                active = key['is_active']
                created = key['created_at']
                last_used = key['last_used_at']
                
                usage_today = f"{req_today}/{daily_lim}"
                active_str = "✓" if active else "✗"
                last_used_str = last_used.strftime("%Y-%m-%d %H:%M") if last_used else "Never"
                
                print(f"{key_id:<5} {prefix:<12} {email:<30} {tier:<12} {usage_today:<10} {active_str:<8} {last_used_str:<20}")
            
        except Exception as e:
            print(f"Error listing keys: {e}")
    
    def deactivate_key(self, key_id: int):
        """Deactivate an API key"""
        try:
            email = ApiKeyRepository.set_active_status(key_id, False)
            if email:
                print(f"✓ API key #{key_id} for {email} has been deactivated.")
            else:
                print(f"✗ API key #{key_id} not found.")
        except Exception as e:
            print(f"Error deactivating key: {e}")
    
    def reactivate_key(self, key_id: int):
        """Reactivate an API key"""
        try:
            email = ApiKeyRepository.set_active_status(key_id, True)
            if email:
                print(f"✓ API key #{key_id} for {email} has been reactivated.")
            else:
                print(f"✗ API key #{key_id} not found.")
        except Exception as e:
            print(f"Error reactivating key: {e}")
    
    def reset_daily_counters(self):
        """Reset daily request counters (run this daily via cron)"""
        try:
            affected = ApiKeyRepository.reset_counters('daily')
            print(f"✓ Reset daily counters for {affected} API keys.")
        except Exception as e:
            print(f"Error resetting counters: {e}")
    
    def reset_monthly_counters(self):
        """Reset monthly request counters (run this monthly via cron)"""
        try:
            affected = ApiKeyRepository.reset_counters('monthly')
            print(f"✓ Reset monthly counters for {affected} API keys.")
        except Exception as e:
            print(f"Error resetting counters: {e}")
    
    def get_usage_stats(self):
        """Get API usage statistics"""
        try:
            stats = ApiKeyRepository.get_stats()
            overall = stats['overall']
            top_users = stats['top_users']
            
            # overall is {total_keys, active_keys, ...}
            
            print(f"""
╔══════════════════════════════════════════════════════════════╗
║                     API USAGE STATISTICS                      ║
╠══════════════════════════════════════════════════════════════╣
║ Total API Keys:        {overall['total_keys']:<40} ║
║ Active Keys:           {overall['active_keys']:<40} ║
║ Requests Today:        {overall['total_requests_today'] or 0:<40} ║
║ Requests This Month:   {overall['total_requests_month'] or 0:<40} ║
╚══════════════════════════════════════════════════════════════╝
            """)
            
            if top_users:
                print("\nTop Users Today:")
                print("-" * 60)
                for user in top_users:
                    # user is dict
                    print(f"  {user['user_email']:<40} {user['requests_today']:>8} requests")
            
        except Exception as e:
            print(f"Error getting stats: {e}")


def main():
    """CLI interface"""
    
    manager = APIKeyManager()
    
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
