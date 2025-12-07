# ZSE Data Ingestion API

A complete pipeline to scrape Zimbabwe Stock Exchange (ZSE) data, store it in PostgreSQL, and serve it via a secured FastAPI.

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
If you need to re-apply it manually (e.g. after schema changes):
```bash
psql -h localhost -U postgres -d zse_db -f schema.sql
```

## Usage

### 1. Generate API Key
You can use the admin script to manage API keys:
```bash
# Create a new key
python admin.py create user@example.com free

# List all keys
python admin.py list
```
**Old method (dev only):**
```bash
python -m app.seed_key
```

### 2. Run Scraper Manually
Trigger an immediate scrape and update:
```bash
python -m app.runner --now
```

### 3. Run API
Start the FastAPI server:
```bash
uvicorn app.api:app --reload
```
- **API Docs**: http://127.0.0.1:8000/docs
- **Authorize**: Click "Authorize" in Swagger UI and enter `test_key_123`.

#### Endpoints
- `GET /api/v1/securities`: List securities.
- `GET /api/v1/securities/{symbol}/prices`: Historical prices.
- `GET /api/v1/market/summary`: Daily market stats.
- `GET /api/v1/market/movers`: Top gainers/losers.

### 4. Scheduler
Run the scheduler daemon:
```bash
python -m app.runner
```

## Development
- **Logs**: Check `zse_scraper.log` for scraper details.
- **Tests**: Run `pytest` (if added).
