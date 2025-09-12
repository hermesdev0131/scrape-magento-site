#!/usr/bin/env python3
"""
Flask server for Magento endpoint scraping that can be triggered by n8n or HTTP
Receives requests, runs scraping, and returns JSON data
"""

from flask import Flask, request, jsonify
import time
from datetime import datetime
import logging
import sys
import requests
from urllib.parse import urlparse

# Import the scraper class from existing module
from scraper import MagentoEndpointScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    # handlers=[
    #     logging.FileHandler('/path/to/logfile.log'),
    #     logging.StreamHandler(sys.stdout)
    # ]
)

app = Flask(__name__)

# Global variable to track scraping status
scraping_status = {
    'is_running': False,
    'last_run': None,
    'last_result': None,
    'error': None
}


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'scraping_status': {
            'is_running': scraping_status['is_running'],
            'last_run': scraping_status['last_run']
        }
    })


@app.route('/scrape', methods=['POST'])
def scrape():
    """Main scraping endpoint - runs scraping and returns results"""

    # Check if scraping is already running
    if scraping_status['is_running']:
        return jsonify({
            'error': 'Scraping is already in progress',
            'status': 'running'
        }), 409
    
    try:
        # Get collection URLs from request (optional)
        data = request.get_json() if request.is_json else {}
        
        # Default collection URLs
        default_urls = [
        ]
        
        collection_urls = data.get('collection_urls', default_urls)
        
        
        # Validate URLs
        if not isinstance(collection_urls, list) or not collection_urls:
            return jsonify({
                'error': 'Invalid collection_urls. Must be a non-empty list of URLs.'
            }), 400
        
        logging.info(f"Received synchronous scraping request for {len(collection_urls)} collections")
        
        # Run scraping synchronously
        scraping_status['is_running'] = True
        scraping_status['error'] = None

        start_ts = time.time()

        try:
            # Build base_url from the first URL (assumes same domain). For mixed domains, could group by domain.
            parsed0 = urlparse(collection_urls[0].strip())
            base_url = f"{parsed0.scheme}://{parsed0.netloc}"
            scraper = MagentoEndpointScraper(base_url)

            # Use main scraper method to handle pagination and extraction per URL
            all_products = scraper.scrape_products(collection_urls)

            # Prepare final result
            duration = round(time.time() - start_ts, 2)
            result = {
                'collection_urls': collection_urls,
                'total_collections': len(collection_urls),
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

            return jsonify(result)
        finally:
            scraping_status['is_running'] = False
            
    except Exception as e:
        error_msg = f"Scraping failed: {str(e)}"
        logging.error(error_msg)
        scraping_status['is_running'] = False
        return jsonify({'error': error_msg, 'status': 'failed'}), 500



@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    print("🚀 Starting Magento Scraper Server...")
    print("Available endpoints:")
    print("  GET  /health    - Health check")
    print("  POST /scrape    - Run scraping and return results")
    print("\nServer starting on http://0.0.0.0:5000")
    print("=" * 60)

    # Run the Flask server
    app.run()
    # app.run(
    #     host='0.0.0.0',  # Listen on all interfaces
    #     port=5000,
    #     debug=False,     # Set to False for production
    #     threaded=True    # Enable threading for concurrent requests
    # )