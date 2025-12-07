# ZSE Data Ingestion API

A complete pipeline to scrape Zimbabwe Stock Exchange (ZSE) data, store it in PostgreSQL, and serve it via a FastAPI.

## Setup

### 1. Prerequisites
- Docker and Docker Compose
- Python 3.9+

### 2. Start Infrastructure
Start the PostgreSQL database and pgAdmin interface:
```bash
docker-compose up -d
```
- **Postgres**: localhost:5432
- **pgAdmin**: localhost:5050 (Login: `admin@admin.com` / `admin`)

### 3. Python Environment
Create a virtual environment and install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Database Setup
The database schema is automatically applied by Docker on the first run. 
If you need to re-apply it manually:
```bash
# Copy schema content and run in pgAdmin Query Tool or via psql
psql -h localhost -U postgres -d zse_db -f schema.sql
```

## Usage

### Run Scraper Manually
Trigger an immediate scrape and update:
```bash
python -m app.runner --now
```

### Run Scraper Scheduler
Starts the process to run daily at 17:00:
```bash
python -m app.runner
```

### Run API
Start the FastAPI server:
```bash
uvicorn app.api:app --reload
```
- API Docs: http://127.0.0.1:8000/docs
- Latest Prices: http://127.0.0.1:8000/prices/latest

## Deployment

### Systemd Service (Linux)
Create `/etc/systemd/system/zse-scraper.service`:
```ini
[Unit]
Description=ZSE Scraper Service
After=network.target

[Service]
User=youruser
WorkingDirectory=/path/to/zse-api
ExecStart=/path/to/zse-api/venv/bin/python -m app.runner
Restart=always

[Install]
WantedBy=multi-user.target
```

### GitHub Actions (CI/CD)
See `.github/workflows/main.yml` (create if needed) to run tests or linting on push.
