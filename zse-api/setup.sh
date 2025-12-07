#!/bin/bash
# ZSE Scraper Setup Script

set -e

echo "=== ZSE Scraper Setup ==="

# Check if we're in the right directory
if [ ! -f "scraper_db.py" ]; then
    echo "ERROR: Must run from zse-api directory"
    exit 1
fi

# Create virtualenv if needed
if [ ! -d "../venv" ]; then
    echo "Creating virtualenv..."
    python3 -m venv ../venv
fi

echo "✓ Found virtualenv at ../venv"

# Install dependencies using venv's pip directly
echo "Installing dependencies..."
../venv/bin/pip install --upgrade pip
../venv/bin/pip install -r requirements.txt

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To run the scraper:"
echo "  ./run.sh"
echo ""
