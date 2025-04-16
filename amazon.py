from selectorlib import Extractor
import requests 
import json 
from time import sleep
import re
import random
import os
from datetime import datetime
import concurrent.futures
from tqdm import tqdm


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Create an Extractor by reading from the YAML file
extractor = Extractor.from_yaml_file(os.path.join(script_dir, 'selectors.yml'))

def get_random_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]
    return random.choice(user_agents)

def get_headers(is_amazon_in=False):
    # Get a random user agent
    user_agent = get_random_user_agent()
    
    # Generate a random session ID
    session_id = ''.join(random.choices('0123456789ABCDEF', k=32))
    
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'TE': 'Trailers',
        'DNT': '1',
        'Cache-Control': 'max-age=0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="92"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Cookie': f'session-id={session_id}; session-token={session_id}',
    }
    
    if is_amazon_in:
        headers['Referer'] = 'https://www.amazon.in/'
        headers['Origin'] = 'https://www.amazon.in'
        headers['Host'] = 'www.amazon.in'
    else:
        headers['Referer'] = 'https://www.amazon.com/'
        headers['Origin'] = 'https://www.amazon.com'
        headers['Host'] = 'www.amazon.com'
    
    return headers

def scrape_reviews_page(asin, max_retries=3):
    """Scrapes the dedicated product reviews page for the top critical review."""
    if not asin:
        print("Cannot scrape reviews page without ASIN.")
        return None

    # Construct the base reviews page URL (without reviewerType parameter)
    # TODO: Handle domain difference (.com vs .in) if needed
    reviews_url = f"https://www.amazon.in/product-reviews/{asin}/"
    print(f"Attempting to scrape reviews page: {reviews_url}")
    is_amazon_in = 'amazon.in' in reviews_url # Assume .in for now

    for attempt in range(max_retries):
        try:
            sleep_time = random.uniform(1, 3)
            sleep(sleep_time)
            headers = get_headers(is_amazon_in) # Use existing header logic

            r = requests.get(reviews_url, headers=headers, timeout=10)

            if r.status_code > 500:
                print(f"Reviews page {reviews_url} may be blocked (Status: {r.status_code})")
                continue
            if not r.text or len(r.text) < 100:
                 print(f"Received empty or very short response from reviews page {reviews_url}")
                 continue

            # Save reviews page HTML for debugging regardless of success for now
            debug_reviews_file = os.path.join(script_dir, 'debug_reviews.html')
            with open(debug_reviews_file, 'w', encoding='utf-8') as f:
                f.write(r.text)
            print(f"Saved reviews page HTML to {debug_reviews_file}")

            # Use the same extractor instance
            review_data = extractor.extract(r.text)

            # Use the specific selector for the reviews page
            if review_data and review_data.get('top_critical_review_on_reviews_page'):
                critical_review_text = review_data['top_critical_review_on_reviews_page'].strip()
                print("Successfully extracted critical review.")
                return critical_review_text
            else:
                print(f"Could not find critical review using selector on page {reviews_url}.")
                # Return None if not found after first successful page load, but HTML is saved
                return None

        except Exception as e:
            print(f"Error scraping reviews page {reviews_url}: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying reviews page scrape (Attempt {attempt + 2})...")
                continue
            else:
                print("Max retries reached for reviews page.")
                return None

    return None # Return None if all retries fail

def scrape(url, max_retries=3):  
    # Check if the URL is for Amazon India
    is_amazon_in = 'amazon.in' in url
    domain = "amazon.in" if is_amazon_in else "amazon.com" # Store domain
    
    for attempt in range(max_retries):
        try:
            # Add a random delay between requests
            sleep_time = random.uniform(1, 3)
            sleep(sleep_time)
            
            # Get headers for this request
            headers = get_headers(is_amazon_in)
            
            # Download the page using requests
            print(f"Downloading {url} (Attempt {attempt + 1}/{max_retries})")
            r = requests.get(url, headers=headers, timeout=10)
            
            # Check if we got blocked
            if r.status_code > 500:
                if "To discuss automated access to Amazon data please contact" in r.text:
                    print(f"Page {url} was blocked by Amazon. Please try using better proxies\n")
                else:
                    print(f"Page {url} must have been blocked by Amazon as the status code was {r.status_code}")
                continue
            
            # Check if we got a valid HTML response
            if not r.text or len(r.text) < 100:
                print(f"Received empty or very short response from {url}")
                continue
                
            # Save the HTML for debugging if needed
            debug_file = os.path.join(script_dir, 'debug.html')
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(r.text)
                
            # Extract data using the selector
            data = extractor.extract(r.text)
            
            # Process the data to ensure all fields are properly formatted
            if data:
                # Extract ASIN from URL if not found in the page
                if not data.get('asin'):
                    if 'asin=' in url:
                        asin_match = re.search(r'asin=([A-Z0-9]{10})', url)
                        if asin_match:
                            data['asin'] = asin_match.group(1)
                    elif '/dp/' in url:
                        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
                        if asin_match:
                            data['asin'] = asin_match.group(1)
                
                # Scrape Reviews Page for Critical Review
                critical_review = None
                if data.get('asin'):
                    # Pass domain to review scraper if needed, but for now assume .in
                    critical_review = scrape_reviews_page(data['asin'])
                data['top_critical_review'] = critical_review # Overwrite main page attempt
                
                # Clean up the top critical review if it exists
                if data.get('top_critical_review'):
                    data['top_critical_review'] = data['top_critical_review'].strip()
                
                # Clean up about_this_item (join list)
                if data.get('about_this_item') and isinstance(data['about_this_item'], list):
                    data['about_this_item'] = "; ".join([item.strip() for item in data['about_this_item'] if item]) # Join list items
                elif data.get('about_this_item'): # Ensure it's a string if not a list
                    data['about_this_item'] = str(data['about_this_item']).strip()
                
                # Ensure all fields exist in the output
                for field in ['title', 'price', 'style_code', 'asin', 'rating', 'number_of_reviews', 
                             'stock_status', 'about_this_item', 'top_critical_review', 'short_description', 
                             'product_description', 'sales_rank']:
                    if field not in data:
                        data[field] = None
                
                # Clean up price field
                if data.get('price'):
                    price_match = re.search(r'([\$₹£]?\d{1,3}(?:[,.]\d{3})*(?:[,.]\d{2})?)', str(data['price']))
                    if price_match:
                        data['price'] = price_match.group(1)
                    else:
                         # Try extracting digits if no currency symbol found
                         digits = re.findall(r'\d+', str(data['price']))
                         if digits:
                            data['price'] = "".join(digits)
                         else:
                            data['price'] = data['price'].strip() # Keep original if no match
                
                # Clean up rating field
                if data.get('rating'):
                    rating_match = re.search(r'(\d+(\.\d+)?)', data['rating'])
                    if rating_match:
                        data['rating'] = rating_match.group(1) + " out of 5"
                
                # Clean up style code (Item model number)
                if data.get('style_code'):
                    # Remove potential leading/trailing whitespace and extra characters
                    data['style_code'] = re.sub(r'^\s*([^\s\w]*)\s*', '', data['style_code']).strip()
                    # Specific trim for the observed issue if needed, but let's try a general clean first
                    # data['style_code'] = data['style_code'].strip().rstrip('1') # Example specific trim
                
                # Clean up stock status
                if data.get('stock_status'):
                    if 'in stock' in data['stock_status'].lower():
                        data['stock_status'] = 'In Stock'
                    elif 'out of stock' in data['stock_status'].lower():
                        data['stock_status'] = 'Out of Stock'
                    elif 'temporarily out of stock' in data['stock_status'].lower():
                        data['stock_status'] = 'Temporarily Out of Stock'
                data['url'] = url
                return data
            else:
                print(f"No data extracted from {url}")
                continue
                
        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            if attempt < max_retries - 1:
                continue
            else:
                return None
    
    return None

if __name__ == '__main__':
    urls = []
    input_file = "urls.txt"
    output_file = 'output.jsonl'
    max_workers = 50 # Number of parallel threads (adjust as needed)

    # Read URLs from the input file
    try:
        with open(input_file, 'r') as urllist:
            urls = [line.strip() for line in urllist if line.strip()]
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        exit()

    if not urls:
        print(f"Input file '{input_file}' is empty. No URLs to process.")
        exit()

    results_list = []
    print(f"Starting scrape for {len(urls)} URLs using up to {max_workers} parallel workers...")

    # Use ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a dictionary mapping future to url for error reporting
        future_to_url = {executor.submit(scrape, url): url for url in urls}

        # Process futures as they complete, showing progress with tqdm
        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urls), desc="Scraping Progress"):
            url = future_to_url[future]
            try:
                data = future.result() # Get result from future
                if data:
                    results_list.append(data)
            except Exception as exc:
                print(f'\nURL {url} generated an exception: {exc}') # Print exceptions

    # Write all collected results to the output file once
    print(f"\nScraping finished. Writing {len(results_list)} results to {output_file}...")
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for item in results_list:
                json.dump(item, outfile, ensure_ascii=False) # Use ensure_ascii=False for wider char support
                outfile.write("\n")
        print("Successfully wrote results.")
    except Exception as e:
        print(f"Error writing results to {output_file}: {e}")
    