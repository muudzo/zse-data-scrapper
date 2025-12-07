#!/bin/bash
# Database Setup Script for ZSE API

set -e

echo "=== ZSE Database Setup ==="
echo ""

# Check if Docker is running
if ! docker ps &> /dev/null; then
    echo "❌ Docker is not running"
    echo ""
    echo "Please start Docker Desktop, then run this script again."
    echo ""
    echo "Alternative: Install PostgreSQL locally"
    echo "  brew install postgresql@13"
    echo "  brew services start postgresql@13"
    exit 1
fi

echo "✓ Docker is running"

# Check if container already exists
if docker ps -a | grep -q zse-postgres; then
    echo "Container 'zse-postgres' already exists"
    
    # Check if it's running
    if docker ps | grep -q zse-postgres; then
        echo "✓ Database is already running"
    else
        echo "Starting existing container..."
        docker start zse-postgres
        echo "✓ Database started"
    fi
else
    echo "Creating new PostgreSQL container..."
    docker run -d \
      --name zse-postgres \
      -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_DB=zse_db \
      -p 5433:5432 \
      postgres:13
    
    echo "✓ Database container created"
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5
fi

# Apply schema
echo ""
echo "Applying database schema..."
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d zse_db -f database_schema.sql 2>&1 | grep -v "already exists" || true

echo ""
echo "=== Database Setup Complete ==="
echo ""
echo "Database connection details:"
echo "  Host: localhost"
echo "  Port: 5433"
echo "  Database: zse_db"
echo "  User: postgres"
echo "  Password: postgres"
echo ""
echo "To stop the database:"
echo "  docker stop zse-postgres"
echo ""
echo "To remove the database:"
echo "  docker rm -f zse-postgres"
echo ""
