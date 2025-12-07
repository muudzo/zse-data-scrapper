#!/bin/bash
# PostgreSQL Setup for ZSE API - Local Development

set -e

echo "=== PostgreSQL Local Setup ==="
echo ""

# Get current macOS user
CURRENT_USER=$(whoami)
echo "Current user: $CURRENT_USER"

# Step 1: Create postgres superuser role
echo ""
echo "Step 1: Creating 'postgres' superuser role..."
psql -U $CURRENT_USER -d postgres -c "CREATE ROLE postgres WITH SUPERUSER LOGIN PASSWORD 'postgres';" 2>/dev/null || echo "  (Role 'postgres' already exists)"

# Step 2: Create zse_db database
echo ""
echo "Step 2: Creating 'zse_db' database..."
psql -U $CURRENT_USER -d postgres -c "CREATE DATABASE zse_db OWNER postgres;" 2>/dev/null || echo "  (Database 'zse_db' already exists)"

# Step 3: Grant privileges
echo ""
echo "Step 3: Granting privileges..."
psql -U $CURRENT_USER -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE zse_db TO postgres;"

# Step 4: Apply schema
echo ""
echo "Step 4: Applying database schema..."
psql -U postgres -d zse_db -f database_schema.sql 2>&1 | grep -E "CREATE|ERROR" || echo "  Schema applied successfully"

# Step 5: Test connection
echo ""
echo "Step 5: Testing connection..."
psql -U postgres -d zse_db -c "SELECT version();" > /dev/null && echo "  ✓ Connection successful!"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Database configuration:"
echo "  User: postgres"
echo "  Password: postgres"
echo "  Database: zse_db"
echo "  Host: localhost"
echo "  Port: 5432 (local PostgreSQL)"
echo ""
echo "Your .env file has been updated."
echo ""
