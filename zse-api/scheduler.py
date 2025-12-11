from apscheduler.schedulers.blocking import BlockingScheduler
from etl import ZSEDataPipeline
import logging
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_job():
    try:
        logger.info("Starting scheduled scrape job...")
        pipeline = ZSEDataPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Scheduled job failed: {e}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Run every weekday at 15:30 (3:30 PM)
    scheduler.add_job(scrape_job, 'cron', day_of_week='mon-fri', hour=15, minute=30, timezone='Africa/Harare')
    
    logger.info("Scheduler started. Waiting for jobs (Mon-Fri 15:30)...")
    
    def signal_handler(sig, frame):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
