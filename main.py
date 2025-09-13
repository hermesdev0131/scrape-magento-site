#!/usr/bin/env python3
"""
FastAPI server for Magento endpoint scraping that can be triggered by n8n or HTTP
Provides synchronous and asynchronous scrape endpoints to avoid request timeouts.
Run with uvicorn:
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, AnyHttpUrl
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import urlparse
import time
import logging

# Import the scraper class from existing module
from scraper import MagentoEndpointScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

app = FastAPI(title="Magento Scraper API")

# Global variable to track scraping status
scraping_status: Dict[str, Any] = {
    'is_running': False,
    'last_run': None,
    'last_result': None,
    'error': None
}


class ScrapeRequest(BaseModel):
    collection_urls: List[AnyHttpUrl]
    # Optional: pass through limiter to keep jobs bounded (wire into scraper if supported)
    max_pages: Optional[int] = None


def run_scrape(urls: List[str], max_pages: Optional[int] = None) -> Dict[str, Any]:
    """Blocking scrape function executed synchronously or in background.
    If your scraper supports max_pages, connect it inside scrape_products.
    """
    try:
        scraping_status['is_running'] = True
        scraping_status['error'] = None

        start_ts = time.time()

        # Build base_url from the first URL (assumes same domain)
        parsed0 = urlparse(urls[0].strip())
        base_url = f"{parsed0.scheme}://{parsed0.netloc}"
        scraper = MagentoEndpointScraper(base_url)

        # Do the scrape; adapt to pass max_pages if supported by your scraper
        all_products = scraper.scrape_products(urls)

        duration = round(time.time() - start_ts, 2)
        result: Dict[str, Any] = {
            'collection_urls': urls,
            'total_collections': len(urls),
            'total_products': len(all_products),
            'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'products': all_products,
            'status': 'completed',
            'mode': 'collection_html',
            'duration_sec': duration,
        }

        scraping_status['last_result'] = {
            'total_collections': result['total_collections'],
            'total_products': result['total_products'],
            'scraped_at': result['scraped_at'],
            'status': result['status'],
            'mode': result['mode'],
            'duration_sec': result['duration_sec'],
        }
        scraping_status['last_run'] = result['scraped_at']

        logging.info(
            f"Scraping completed successfully in {result['mode']} mode. Total products: {result['total_products']}"
        )

        return result
    except Exception as e:
        error_msg = f"Scraping failed: {str(e)}"
        logging.error(error_msg)
        scraping_status['error'] = error_msg
        scraping_status['last_result'] = {'status': 'failed'}
        raise
    finally:
        scraping_status['is_running'] = False


@app.get('/health')
def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'scraping_status': {
            'is_running': scraping_status['is_running'],
            'last_run': scraping_status['last_run']
        }
    }


@app.post('/scrape')
def scrape(req: ScrapeRequest):
    """Main scraping endpoint - runs scraping synchronously and returns results"""
    if scraping_status['is_running']:
        raise HTTPException(status_code=409, detail='Scraping is already in progress')

    urls = [str(u) for u in req.collection_urls]
    if not isinstance(urls, list) or not urls:
        raise HTTPException(status_code=400, detail='Invalid collection_urls. Must be a non-empty list of URLs.')

    logging.info(f"Received synchronous scraping request for {len(urls)} collections")

    result = run_scrape(urls, req.max_pages)
    return result


@app.post('/scrape_async')
def scrape_async(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """Starts scraping in the background and returns immediately with status."""
    if scraping_status['is_running']:
        return {'status': 'running'}

    urls = [str(u) for u in req.collection_urls]
    if not isinstance(urls, list) or not urls:
        raise HTTPException(status_code=400, detail='Invalid collection_urls. Must be a non-empty list of URLs.')

    logging.info(f"Received async scraping request for {len(urls)} collections")

    # queue background task
    background_tasks.add_task(run_scrape, urls, req.max_pages)
    return {'status': 'accepted'}


@app.get('/status')
def status():
    return {
        'is_running': scraping_status['is_running'],
        'last_run': scraping_status['last_run'],
        'last_result': scraping_status['last_result'],
        'error': scraping_status['error'],
    }


if __name__ == '__main__':
    # Local dev server start (uvicorn)
    print("🚀 Starting Magento Scraper FastAPI Server...")
    print("Available endpoints:")
    print("  GET  /health       - Health check")
    print("  POST /scrape       - Run scraping synchronously and return results")
    print("  POST /scrape_async - Start scraping in background and return immediately")
    print("  GET  /status       - Check scrape status and last result")
    print("\nServer starting on http://0.0.0.0:8000")
    print("=" * 60)

    # Import here to keep uvicorn optional at import-time
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=False)