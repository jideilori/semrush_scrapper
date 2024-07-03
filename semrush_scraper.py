import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import concurrent.futures
import time
from utils import *

# Runs in 40 seconds
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': '*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'text/plain;charset=UTF-8',
    'Origin': 'https://www.semrush.com',
    'Connection': 'keep-alive',
    'Referer': 'https://www.semrush.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'Priority': 'u=1',
}

url = 'https://www.semrush.com/trending-websites/us/all'
categories, column_names = fetch_trending_website_data(url, headers)
all_df = pd.DataFrame(columns=column_names)




# Function to scrape a category
def scrape_category(category):
    category_url = f'https://www.semrush.com/trending-websites/us/{category}'
    cat_response = requests.get(category_url, headers=headers)
    time.sleep(1)  # Be respectful of server limits
    cat_soup = BeautifulSoup(cat_response.text, 'html.parser')
    cat_json_data = extract_json_from_text(cat_soup)

    if cat_json_data and 'data' in cat_json_data and 'domains' in cat_json_data['data']:
        cat_df = pd.DataFrame(cat_json_data['data']['domains'])
        cat_df['category'] = category
        return cat_df
    else:
        return pd.DataFrame()  # Return an empty DataFrame if JSON is missing or incomplete

# Concurrently scrape all categories
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(scrape_category, category) for category in categories]
    all_df = pd.concat([future.result() for future in concurrent.futures.as_completed(futures)], ignore_index=True)

# Save to CSV
all_df.to_csv('semrush_listed_sites_concurrent.csv', index=False)