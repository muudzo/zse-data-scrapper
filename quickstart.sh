#!/bin/bash
# Quick Start Script - Run from parent directory

echo "🚀 ZSE API Quick Start"
echo ""

# Check current directory
if [ ! -d "zse-api" ]; then
    echo "❌ Error: Must run from 'zse data scrapper' directory"
    echo "Current directory: $(pwd)"
    echo ""
    echo "Run this command first:"
    echo "  cd ~/Desktop/zse\ data\ scrapper"
    exit 1
fi

echo "✓ Correct directory"
echo ""

# Navigate to zse-api
cd zse-api

# Activate virtualenv
echo "Activating virtualenv..."
source ../venv/bin/activate

echo ""
echo "=== Quick Commands ==="
echo ""
echo "Test database:"
echo "  python test_db_connection.py"
echo ""
echo "Run scraper:"
echo "  python scraper_db.py"
echo ""
echo "Start API:"
echo "  uvicorn main:app --reload"
echo ""
echo "Create API key:"
echo "  python admin.py create your@email.com free"
echo ""
