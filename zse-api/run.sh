#!/bin/bash
# ZSE Scraper Runner - Single command to run everything

set -e

# Check virtualenv exists
if [ ! -d "../venv" ]; then
    echo "ERROR: Virtualenv not found. Run ./setup.sh first"
    exit 1
fi

# Run scraper using venv's python
../venv/bin/python scraper_db.py
