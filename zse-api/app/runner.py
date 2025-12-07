import schedule
import time
import logging
import sys
from app.logging_conf import setup_logging
from app.scraper import ZSEScraper
from app.insert import upsert_securities, upsert_prices, upsert_market_activity

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

def run_job():
    """Main scraping job"""
    logger.info("Starting ZSE scrape job...")
    try:
        scraper = ZSEScraper()
        data = scraper.scrape_all()
        
        if not data:
            logger.warning("No data scraped. Aborting.")
            return

        logger.info(f"Scraped data successfully. Source: {data['source']}")

        # 1. Upsert Securities (Reference Data)
        upsert_securities(data)
        logger.info("Securities upserted.")

        # 2. Upsert Market Activity
        upsert_market_activity(data)
        logger.info("Market activity upserted.")

        # 3. Upsert Prices
        # Use trade date from market activity if available, else today
        trade_date = data.get('market_activity', {}).get('trade_date')
        upsert_prices(data, trade_date_str=trade_date)
        logger.info("Prices upserted.")
        
        logger.info("Job completed successfully.")
        
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_job()
        return

    # Schedule: Run every weekday at 17:00 (5 PM)
    # ZSE market closes around 3-4 PM usually?
    schedule.every().monday.at("17:00").do(run_job)
    schedule.every().tuesday.at("17:00").do(run_job)
    schedule.every().wednesday.at("17:00").do(run_job)
    schedule.every().thursday.at("17:00").do(run_job)
    schedule.every().friday.at("17:00").do(run_job)

    logger.info("Scheduler started. Waiting for jobs...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
