#!/usr/bin/env python3
"""
Fast Magento Single-Endpoint Product Scraper
Optimized scraper using only the '/catalogsearch/result/index' endpoint for maximum efficiency.
Extracts product information (name, price, size) with priority size selection (3.5kg, 16oz, 1gallon).
Achieves comprehensive site coverage through diverse search terms and category-like queries.
"""

import requests
import json
import time
from urllib.parse import urljoin
from typing import List, Dict, Optional


class MagentoEndpointScraper:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

    def discover_endpoints(self) -> Dict[str, str]:
        """Test the single catalogsearch endpoint"""
        endpoints = {}
        endpoint = '/catalogsearch/result/index'
        
        print("🔍 Testing catalogsearch endpoint...")
        
        try:
            url = urljoin(self.base_url, endpoint)
            print(f"Testing: {url}")

            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                endpoints[endpoint] = url
                print(f"✅ Found working endpoint: {endpoint}")
            else:
                print(f"❌ Endpoint not available: {endpoint} (Status: {response.status_code})")

        except requests.exceptions.RequestException as e:
            print(f"❌ Error testing {endpoint}: {str(e)}")

        return endpoints

    def try_graphql_query(self) -> Optional[List[Dict]]:
        """GraphQL disabled - using only catalogsearch/result/index endpoint"""
        print("⚠️ GraphQL disabled - using only catalogsearch/result/index endpoint")
        return None

    def try_rest_api(self, endpoint: str) -> Optional[List[Dict]]:
        """REST API disabled - using only catalogsearch/result/index endpoint"""
        print("⚠️ REST API disabled - using only catalogsearch/result/index endpoint")
        return None

    def try_search_endpoint(self) -> Optional[List[Dict]]:
        """Get products using comprehensive search terms via catalogsearch endpoint"""
        search_terms = [
            'oil', 'organic', 'butter', 'essential', 'virgin', 'refined', 
            'cold pressed', 'extract', 'powder', 'wax', 'seed', 'nut', 
            'coconut', 'olive', 'almond', 'jojoba', 'argan', 'sweet',
            'natural', 'pure', 'unrefined', 'carrier', 'base', 'massage',
            'cosmetic', 'food grade', 'therapeutic', 'aromatherapy',
            'moisturizing', 'anti-aging', 'nourishing', 'hydrating'
        ]

        all_search_products = []

        for term in search_terms:
            try:
                # Only use catalogsearch/result/index endpoint
                search_path = f'/catalogsearch/result/index/?q={term}'
                url = urljoin(self.base_url, search_path)
                response = self.session.get(url, timeout=15)

                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and 'products' in str(data):
                            print(f"✅ Found search endpoint: {search_path}")
                            search_products = self.parse_search_results(data)
                            if search_products:
                                all_search_products.extend(search_products)
                    except:
                        # Not JSON, try to extract from HTML content
                        if 'product' in response.text.lower() and 'starting at' in response.text.lower():
                            print(f"✅ Found search page with products: {search_path}")
                            search_products = self.extract_from_html_json(response.text)
                            if search_products:
                                all_search_products.extend(search_products)

            except Exception as e:
                print(f"⚠️ Search error for {term}: {str(e)}")
                continue

        return all_search_products if all_search_products else None

    def try_category_endpoints(self) -> Optional[List[Dict]]:
        """Get products using category-like search terms via catalogsearch endpoint"""
        # Convert category paths to search terms for catalogsearch endpoint
        category_terms = [
            'essential oil',
            'plant oil', 
            'carrier oil',
            'base oil',
            'massage oil',
            'cosmetic oil',
            'food grade oil',
            'butter',
            'natural butter',
            'cosmetic wax',
            'food grade wax',
            'chocolate',
            'cocoa',
            'coffee',
            'salt',
            'natural salt',
            'organic sweetener',
            'botanical extract',
            'herbal extract',
            'fruit powder',
            'vegetable powder',
            'emulsifier',
            'natural emulsifier',
            'food thickener',
            'raw material',
            'specialty ingredient',
            'organic product',
            'natural product',
            'cosmetic ingredient',
            'food ingredient'
        ]

        all_products = []

        for category_term in category_terms:
            try:
                # Use catalogsearch/result/index with category-like terms and pagination
                for page in range(1, 11):  # Enhanced pagination: 10 pages vs 3
                    search_path = f'/catalogsearch/result/index/?q={category_term.replace(" ", "+")}&p={page}'
                    url = urljoin(self.base_url, search_path)
                    response = self.session.get(url, timeout=15)

                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if isinstance(data, dict):
                                print(f"✅ Found category endpoint: {search_path}")
                                category_products = self.parse_category_results(
                                    data)
                                if category_products:
                                    all_products.extend(category_products)
                                    continue
                        except:
                            pass

                        # Try HTML extraction
                        if 'product' in response.text.lower():
                            category_products = self.extract_from_html_json(response.text)
                            if category_products:
                                print(f"✅ Extracted {len(category_products)} products from HTML structure")
                                print(f"📄 Page {page}: Found {len(category_products)} products")
                                all_products.extend(category_products)
                                
                                # Enhanced threshold: 500 vs 50
                                if len(all_products) >= 500:
                                    print(f"🎯 Reached threshold of 500 products, continuing to next category...")
                                    break
                            else:
                                # No products found on this page, probably end of pagination
                                break
                        else:
                            # No products on this page
                            break
                    else:
                        # Page not found, try next category
                        break
                        
                    # Small delay between pages
                    time.sleep(0.5)

            except Exception as e:
                print(f"⚠️ Category error for {category_term}: {str(e)}")
                continue

        return all_products if all_products else None

    def extract_from_html_json(self,
                               html_content: str) -> Optional[List[Dict]]:
        """Extract product data from JSON embedded in HTML"""
        import re

        # First try to extract JSON patterns
        json_patterns = [
            r'"items":\s*(\[.*?\])',
            r'"products":\s*(\[.*?\])',
            r'var\s+productData\s*=\s*(\{.*?\});',
            r'window\.catalog\s*=\s*(\{.*?\});',
            r'"spConfig":\s*(\{.*?\})',
        ]

        for pattern in json_patterns:
            matches = re.findall(pattern, html_content, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list) and len(data) > 0:
                        print(
                            f"✅ Extracted {len(data)} products from HTML JSON")
                        return data
                    elif isinstance(data, dict) and 'products' in data:
                        products = data['products']
                        if isinstance(products, list):
                            print(
                                f"✅ Extracted {len(products)} products from HTML JSON"
                            )
                            return products
                except:
                    continue

        # If JSON extraction fails, try HTML structure parsing
        return self.extract_from_html_structure(html_content)

    def extract_from_html_structure(self,
                                    html_content: str) -> Optional[List[Dict]]:
        """Extract products from Magento's actual HTML structure"""
        import re

        products = []

        # Look for product list items with class "product item product-item"
        product_li_pattern = r'<li[^>]*class="[^"]*product[^"]*item[^"]*product-item[^"]*"[^>]*>(.*?)</li>'
        product_matches = re.findall(product_li_pattern, html_content,
                                     re.DOTALL | re.IGNORECASE)

        for product_html in product_matches:
            product_info = self.parse_product_html(product_html)
            if product_info:
                products.append(product_info)

        if products:
            print(f"✅ Extracted {len(products)} products from HTML structure")
            return products

        # Fallback: try simpler pattern for product names and prices
        return self.extract_simple_product_pattern(html_content)

    def parse_product_html(self, product_html: str) -> Optional[Dict]:
        """Parse individual product HTML to extract name, price, URL"""
        import re

        product_info = {
            'name': 'Unknown',
            'price': 'N/A',
            'url': 'N/A',
            'sku': 'N/A',
            'size': 'Various sizes available'
        }

        # Extract product URL and name from anchor tags
        link_pattern = r'<a[^>]*href="([^"]*\.html)"[^>]*>([^<]*)</a>'
        link_matches = re.findall(link_pattern, product_html, re.IGNORECASE)

        for url, potential_name in link_matches:
            # Skip empty names or very short names
            clean_name = potential_name.strip()
            if len(clean_name) > 3 and not clean_name.lower() in [
                    'view', 'details', 'more'
            ]:
                product_info['url'] = url
                product_info['name'] = clean_name
                product_info['sku'] = self.extract_sku_from_url(url)
                break

        # Extract price
        price_patterns = [
            r'starting at[^$]*\$([0-9.,]+)', r'as low as[^$]*\$([0-9.,]+)',
            r'price[^$]*\$([0-9.,]+)', r'\$([0-9.,]+)'
        ]

        for pattern in price_patterns:
            price_match = re.search(pattern, product_html, re.IGNORECASE)
            if price_match:
                product_info['price'] = f"${price_match.group(1)}"
                break

        # Only return if we found a valid product name
        return product_info if product_info['name'] != 'Unknown' else None

    def extract_simple_product_pattern(
            self, html_content: str) -> Optional[List[Dict]]:
        """Fallback extraction using simpler patterns"""
        import re

        products = []

        # Look for lines that contain product names and prices
        lines = html_content.split('\n')
        current_product = {}

        for i, line in enumerate(lines):
            # Look for HTML links that might be product names
            name_match = re.search(
                r'<a[^>]*href="[^"]*\.html"[^>]*>([^<]+)</a>', line)
            if name_match:
                if current_product and 'name' in current_product:
                    products.append(current_product)

                product_name = name_match.group(1).strip()
                # Skip navigation links
                if (len(product_name) > 3 and not product_name.lower() in [
                        'view', 'details', 'more', 'home', 'contact', 'about',
                        'blog'
                ]):
                    current_product = {
                        'name': product_name,
                        'price': 'N/A',
                        'url': 'N/A',
                        'sku': 'N/A',
                        'size': 'Various sizes available'
                    }

            # Look for price patterns in nearby lines
            price_match = re.search(r'Starting at.*?\$([\d\.]+)', line)
            if price_match and current_product:
                current_product['price'] = f"${price_match.group(1)}"

        # Add the last product if exists
        if current_product and 'name' in current_product:
            products.append(current_product)

        if products:
            print(
                f"✅ Extracted {len(products)} products using simple pattern matching"
            )
            return products

        return None

    def extract_sku_from_url(self, url: str) -> str:
        """Extract SKU from product URL"""
        import re

        # Try to extract SKU from common Magento URL patterns
        sku_patterns = [
            r'/([a-zA-Z0-9-]+)\.html$',  # Last part before .html
            r'-([a-zA-Z]\d+)\.html$',  # Pattern like -s0990.html
            r'/([^/]+)$'  # Last URL segment
        ]

        for pattern in sku_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        return 'N/A'

    def parse_search_results(self, data: Dict) -> List[Dict]:
        """Parse search results into product format"""
        products = []

        # Handle different search result formats
        if 'products' in data:
            products = data['products']
        elif 'items' in data:
            products = data['items']
        elif 'suggestions' in data:
            products = data['suggestions']

        return products if isinstance(products, list) else []

    def parse_category_results(self, data: Dict) -> List[Dict]:
        """Parse category results into product format"""
        products = []

        # Handle different category result formats
        if 'products' in data:
            products = data['products']
        elif 'items' in data:
            products = data['items']
        elif 'productList' in data:
            products = data['productList']

        return products if isinstance(products, list) else []

    def fetch_product_variants(self, product_url: str) -> Dict:
        """Fetch individual product page to extract size variants"""
        import re

        try:
            full_url = urljoin(self.base_url + '/', product_url.strip())

            response = self.session.get(full_url, timeout=15)
            if response.status_code == 200:
                content = response.text

                # Look for grouped product table
                table_pattern = r'<table[^>]*grouped[^>]*>.*?</table>'
                table_match = re.search(table_pattern, content,
                                        re.DOTALL | re.IGNORECASE)

                if table_match:
                    table_html = table_match.group()

                    # Extract rows with size and price data
                    row_pattern = r'<tr[^>]*>.*?</tr>'
                    rows = re.findall(row_pattern, table_html, re.DOTALL)

                    variants = []
                    for row in rows:
                        # Look for size patterns
                        size_patterns = [
                            r'(\d+(?:\.\d+)?\s*(?:oz|lb|lbs|gal|kg|g|fl\s*oz|ounce|pound|gallon|kilogram))',
                            r'(\d+(?:\.\d+)?\s*(?:ml|liter|litre|l))'
                        ]

                        size_found = None
                        for pattern in size_patterns:
                            size_match = re.search(pattern, row, re.IGNORECASE)
                            if size_match:
                                size_found = size_match.group(1).strip()
                                break

                        # Look for price in the same row or nearby
                        price_patterns = [
                            r'\$([0-9,]+\.?[0-9]*)', r'(\d+\.?\d*)\s*USD',
                            r'price["\']:\s*["\']?([0-9,]+\.?[0-9]*)'
                        ]

                        price_found = None
                        for pattern in price_patterns:
                            price_match = re.search(pattern, row,
                                                    re.IGNORECASE)
                            if price_match:
                                price_found = price_match.group(1)
                                break

                        if size_found:
                            variants.append({
                                'size':
                                size_found,
                                'price':
                                f"${price_found}" if price_found else "N/A"
                            })

                    if variants:
                        # Choose the smallest size variant
                        def normalize_size(size_text: str) -> tuple[float, str]:
                            s = size_text.lower().replace(' ', '')
                            # volume units to ml
                            import re
                            m = re.search(r'([0-9]+(?:\.[0-9]+)?)', s)
                            if not m:
                                return (float('inf'), 'unknown')
                            val = float(m.group(1))
                            if 'floz' in s or 'fl.oz' in s:
                                return (val * 29.5735, 'volume')
                            if 'ml' in s:
                                return (val, 'volume')
                            if 'liter' in s or 'litre' in s or s.endswith('l'):
                                return (val * 1000.0, 'volume')
                            if 'gal' in s or 'gallon' in s:
                                return (val * 3785.41, 'volume')
                            # weight units to grams
                            if re.search(r'\boz\b', s) and 'floz' not in s:
                                return (val * 28.3495, 'weight')
                            if 'lb' in s or 'lbs' in s or 'pound' in s:
                                return (val * 453.592, 'weight')
                            if 'kg' in s or 'kilogram' in s:
                                return (val * 1000.0, 'weight')
                            if 'g' in s:
                                return (val, 'weight')
                            return (float('inf'), 'unknown')
                        best = None
                        best_norm = float('inf')
                        for v in variants:
                            norm, _family = normalize_size(v['size'])
                            if norm < best_norm:
                                best = v
                                best_norm = norm
                        if best:
                            print(f"  🎯 Smallest size: {best['size']}")
                            return best
                        # Fallback
                        print(f"  📦 Using available size: {variants[0]['size']}")
                        return variants[0]

                # Fallback: look for any size mentions in product description
                size_patterns = [
                    r'(\d+(?:\.\d+)?\s*(?:oz|lb|lbs|gal|kg|g|fl\s*oz))',
                    r'(\d+(?:\.\d+)?\s*(?:ml|liter|litre|l))'
                ]

                for pattern in size_patterns:
                    size_match = re.search(pattern, content, re.IGNORECASE)
                    if size_match:
                        # Look for nearby price
                        size_context = content[max(0,
                                                   size_match.start() -
                                                   200):size_match.end() + 200]
                        price_match = re.search(r'\$([0-9,]+\.?[0-9]*)',
                                                size_context)

                        return {
                            'size':
                            size_match.group(1).strip(),
                            'price':
                            f"${price_match.group(1)}"
                            if price_match else "N/A"
                        }

        except Exception as e:
            print(f"⚠️ Error fetching variants for {product_url}: {e}")

        return {'size': 'Various sizes available', 'price': 'N/A'}

    def extract_product_info(self,
                             product_data: Dict,
                             source: str = "unknown") -> Dict:
        """Extract standardized product information"""
        product_info = {
            'name': 'Unknown',
            'price': 'N/A',
            'size': 'N/A'
        }

        if source == 'graphql':
            product_info['name'] = product_data.get('name', 'Unknown')

            # Extract price from GraphQL structure
            price_range = product_data.get('price_range', {})
            min_price = price_range.get('minimum_price', {})
            final_price = min_price.get('final_price', {})
            if final_price:
                currency = final_price.get('currency', '')
                value = final_price.get('value', 0)
                product_info['price'] = f"{currency} {value}"

            # Look for size in custom attributes
            custom_attrs = product_data.get('custom_attributes', [])
            for attr in custom_attrs:
                if attr.get('attribute_code',
                            '').lower() in ['size', 'weight', 'volume']:
                    product_info['size'] = str(attr.get('value', 'N/A'))
                    break

        elif source == 'rest':
            product_info['name'] = product_data.get('name', 'Unknown')
            product_info['price'] = f"${product_data.get('price', 'N/A')}"

            # Look for size in custom attributes
            custom_attrs = product_data.get('custom_attributes', [])
            for attr in custom_attrs:
                if attr.get('attribute_code',
                            '').lower() in ['size', 'weight', 'volume']:
                    product_info['size'] = str(attr.get('value', 'N/A'))
                    break

        elif source in ['search', 'category']:
            # Handle search and category results that might have different structures
            product_info['name'] = product_data.get(
                'name') or product_data.get('title') or product_data.get(
                    'label', 'Unknown')

            # Try different price fields
            price = (product_data.get('price')
                     or product_data.get('final_price')
                     or product_data.get('regular_price')
                     or product_data.get('special_price') or 'N/A')
            if isinstance(price, (int, float)):
                product_info['price'] = f"${price}"
            else:
                product_info['price'] = str(price)

            # Look for size/weight/volume information
            size_fields = ['size', 'weight', 'volume', 'capacity', 'amount']
            for field in size_fields:
                if field in product_data:
                    product_info['size'] = str(product_data[field])
                    break

            # Check custom attributes if available
            if 'custom_attributes' in product_data:
                custom_attrs = product_data['custom_attributes']
                if isinstance(custom_attrs, list):
                    for attr in custom_attrs:
                        if attr.get('attribute_code',
                                    '').lower() in size_fields:
                            product_info['size'] = str(attr.get(
                                'value', 'N/A'))
                            break
                elif isinstance(custom_attrs, dict):
                    for field in size_fields:
                        if field in custom_attrs:
                            product_info['size'] = str(custom_attrs[field])
                            break

        # Fetch real size variants from individual product page if we have a URL on the product_data
        if product_info['size'] in ['N/A', 'Various sizes available'] and source in ['search', 'category']:
            product_url = product_data.get('url') or product_data.get('link') or product_data.get('product_url')
            if product_url:
                print(
                    f"🔍 Fetching size variants for {product_info['name']}...")

                variant_data = self.fetch_product_variants(product_url)
                if variant_data['size'] != 'Various sizes available':
                    product_info['size'] = variant_data['size']
                    # Update price with variant-specific price if available
                    if variant_data['price'] != 'N/A':
                        product_info['price'] = variant_data['price']
                    print(
                        f"  ✅ Found size: {variant_data['size']} - {variant_data['price']}"
                    )
                else:
                    print(f"  ❌ No size variants found")

        return product_info

    
    def extract_with_pagination(self,
                                base_url: str,
                                max_pages: int = 5) -> List[Dict]:
        """Extract products from multiple pages of a category or search"""
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                # Add pagination parameter
                if '?' in base_url:
                    url = f"{base_url}&p={page}"
                else:
                    url = f"{base_url}?p={page}"

                response = self.session.get(url, timeout=15)

                if response.status_code == 200:
                    if 'product' in response.text.lower(
                    ) and 'starting at' in response.text.lower():
                        page_products = self.extract_from_html_json(
                            response.text)
                        if page_products:
                            all_products.extend(page_products)
                            print(
                                f"📄 Page {page}: Found {len(page_products)} products"
                            )
                        else:
                            # No products found, likely reached end
                            break
                    else:
                        # No product content, reached end
                        break
                else:
                    # Page not available
                    break

                # Small delay to be respectful
                time.sleep(0.5)

            except Exception as e:
                print(f"⚠️ Error on page {page}: {str(e)}")
                break

        return all_products

    def scrape_products(self, urls: Optional[List[str]] = None) -> List[Dict]:
        """Scrape products using provided page/category URLs. Falls back to defaults if none provided."""
        print(f"🚀 Starting fast endpoint-based scraping for: {self.base_url}")
        start_time = time.time()

        products: List[Dict] = []

        # If no URLs provided, keep the previous behavior (empty/default list)
        if urls is None:
            urls = []

        for url in urls:
            try:
                paginated_products = self.extract_with_pagination(url, max_pages=30)
                print(f"📦 Total paginated products found from {url}: {len(paginated_products)}")
                if paginated_products:
                    for product in paginated_products:
                        # Avoid duplicates by checking if product already exists
                        if not any(p.get('name') == product.get('name') for p in products):
                            product_info = self.extract_product_info(product, 'category')
                            products.append(product_info)
            except Exception as e:
                print(f"⚠️ Pagination error for {url}: {str(e)}")
                continue

        elapsed_time = time.time() - start_time
        print(f"\n⚡ Scraping completed in {elapsed_time:.2f} seconds")
        print(f"📦 Total products found: {len(products)}")

        return products

    def save_to_json(self,
                     products: List[Dict],
                     filename: str = "products.json"):
        """Save products to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved {len(products)} products to {filename}")


def main():
    """Main execution function"""
    url = "https://bulknaturaloils.com"

    print("🔥 Magento Endpoint-Based Product Scraper")
    print("=" * 50)

    scraper = MagentoEndpointScraper(url)

    # Scrape products using endpoints
    products = scraper.scrape_products()

    if products:
        # Display first few products
        print("\n📋 Sample Products:")
        print("-" * 80)
        print(f"{'Name':<50} {'Price':<15} {'Size':<15}")
        print("-" * 80)

        for i, product in enumerate(products[:10]):  # Show first 10
            name = product['name'][:47] + "..." if len(
                product['name']) > 50 else product['name']
            print(
                f"{name:<50} {product['price']:<15} {product['size']:<15}"
            )

        if len(products) > 10:
            print(f"... and {len(products) - 10} more products")

        # Save to file
        scraper.save_to_json(products)

        print(
            f"\n✅ Successfully scraped {len(products)} products using endpoint-based approach!"
        )
        print("🚀 This method is much faster than HTML parsing!")

    else:
        print("\n❌ No products could be retrieved using endpoint methods.")
        print(
            "💡 The site might require authentication or have different API structure."
        )


if __name__ == "__main__":
    main()
