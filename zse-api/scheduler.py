from apscheduler.schedulers.blocking import BlockingScheduler
from scraper_db import ZSEDataPipeline
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load env if needed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE_URL = os.getenv("DATABASE_URL")

def scrape_job():
    logger.info("Running scheduled scrape...")
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set")
        return
        
    pipeline = ZSEDataPipeline(DATABASE_URL)
    pipeline.run()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Run every weekday at 15:30 (3:30 PM)
    scheduler.add_job(scrape_job, 'cron', day_of_week='mon-fri', hour=15, minute=30, timezone='Africa/Harare')
    
    logger.info("Scheduler started. Waiting for jobs...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
