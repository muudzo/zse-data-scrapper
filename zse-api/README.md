# ZSE Market Data API

A complete pipeline to scrape Zimbabwe Stock Exchange (ZSE) data, store it in PostgreSQL, and serve it via a secured FastAPI.

## Quick Start

### 1. Install Dependencies
```bash
cd zse-api
./setup.sh
```

### 2. Setup Database
**Option A: Using Docker (Recommended)**
```bash
# Start Docker Desktop first, then:
./setup_db.sh
```

**Option B: Local PostgreSQL**
```bash
brew install postgresql@13
brew services start postgresql@13
createdb zse_db
psql -d zse_db -f database_schema.sql
```

### 3. Run Scraper
```bash
./run.sh
```

### 4. Start API Server
```bash
source ../venv/bin/activate
uvicorn main:app --reload
```

Visit: http://localhost:8000/docs

## API Key Management

Create an API key:
```bash
source ../venv/bin/activate
python admin.py create user@example.com free
```

## Project Structure

```
zse-api/
├── setup.sh          # Install dependencies
├── setup_db.sh       # Setup database
├── run.sh            # Run scraper
├── scraper.py        # ZSE scraper
├── scraper_db.py     # Database pipeline
├── main.py           # FastAPI application
├── admin.py          # API key management
├── scheduler.py      # Scheduled jobs
└── database_schema.sql
```

## Troubleshooting

**Docker not running:**
- Start Docker Desktop application
- Run `./setup_db.sh` again

**Database connection failed:**
- Check if PostgreSQL is running: `docker ps`
- Verify connection: `psql -h localhost -U postgres -d zse_db`

**Module not found:**
- Activate virtualenv: `source ../venv/bin/activate`
- Or use the scripts: `./setup.sh` then `./run.sh`
