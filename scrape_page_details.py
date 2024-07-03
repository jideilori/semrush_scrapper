import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import concurrent.futures
import time
import csv
import random
from utils import *

# Scraping takes around 3 hours





def get_site_data(domain_name):
    url = f'https://www.semrush.com/website/{domain_name}/overview/?source=trending-websites'
    random_number = random.randint(2, 4)
    time.sleep(random_number)  # Be respectful of server limits
    site_response = requests.get(url, headers=None)
    site_soup = BeautifulSoup(site_response.text, 'html.parser')

    script_tag = site_soup.find('script', id='__NEXT_DATA__')

    if not script_tag or not script_tag.string:
        print(f"No valid script tag found for {domain_name}")
        return None

    data = json.loads(script_tag.string)



    try:
      site_data = data
    except KeyError as e:
        print(f"Error: {e}")
        site_data = None

    try:
        page_props = data['props']['pageProps']['page']
    except KeyError as e:
        print(f"Error: {e}")
        page_props = {}

    try:
        categories = [i['name'] for i in page_props.get('categories', [])]
    except KeyError as e:
        print(f"Error: {e}")
        categories = []

    try:
        global_rank = page_props['trafficStats']['globalRank']['value']
    except KeyError as e:
        print(f"Error: {e}")
        global_rank = None

    try:
        country_rank = page_props['trafficStats']['countryRank']['value']
    except KeyError as e:
        print(f"Error: {e}")
        country_rank = None

    try:
        visits_no = page_props['trafficStats']['visits']['value']
    except KeyError as e:
        print(f"Error: {e}")
        visits_no = None

    try:
        authority_score = page_props['trafficStats']['authorityScore']['value']
    except KeyError as e:
        print(f"Error: {e}")
        authority_score = None

    try:
        pages_per_visit = page_props['visitorEngagement']['pagesPerVisit']['value']
    except KeyError as e:
        print(f"Error: {e}")
        pages_per_visit = None

    try:
        time_on_site = (page_props['visitorEngagement']['timeOnSite']['value'] / 60)
    except KeyError:
        time_on_site = None

    try:
        value_diff_percent = page_props['visitorEngagement']['visits']['valueDiffPercent']
    except KeyError:
        value_diff_percent = None

    try:
        calc = time_on_site * value_diff_percent
    except (TypeError, KeyError) as e:
        print(f"Error: {e}")
        calc = None

    try:
        avg_visit_duration = round((time_on_site - calc), 2)
    except KeyError as e:
        print(f"Error: {e}")
        avg_visit_duration = None

    try:
        bounce_rate = page_props['visitorEngagement']['bounceRate']['value'] * 100
    except KeyError as e:
        print(f"Error: {e}")
        bounce_rate = None

    try:
        traffic_by_country = page_props['traffic_by_country']
    except KeyError as e:
        print(f"Error: {e}")
        traffic_by_country = None

    try:
        traffic_by_device = page_props['traffic_by_device']['traffic_by_device_history'][0]
    except (KeyError, IndexError) as e:
        print(f"Error: {e}")
        traffic_by_device = None

    try:
        desktop_device = round(((traffic_by_device['desktop_visits'] / traffic_by_device['visits']) * 100), 2)
    except KeyError as e:
        print(f"Error: {e}")
        desktop_device = None

    try:
        mobile_device = round(((traffic_by_device['mobile_visits'] / traffic_by_device['visits']) * 100), 2)
    except KeyError as e:
        print(f"Error: {e}")
        mobile_device = None

    try:
        competitors = page_props['competitors']
    except KeyError as e:
        print(f"Error: {e}")
        competitors = None

    try:
        organic_traffic = page_props['traffic_overview']['traffic_organic']['value']
    except KeyError as e:
        print(f"Error: {e}")
        organic_traffic = None

    try:
        paid_traffic = page_props['traffic_overview']['traffic_paid']['value']
    except KeyError as e:
        print(f"Error: {e}")
        paid_traffic = None

    try:
        backlinks = page_props['backlinkAnalytics']['backlinks']['value']
    except KeyError as e:
        print(f"Error: {e}")
        backlinks = None

    try:
        referring_domains = page_props['backlinkAnalytics']['referringDomains']['value']
    except KeyError as e:
        print(f"Error: {e}")
        referring_domains = None

    result = {
        'domain_name': domain_name,
        'global_rank': global_rank,
        'country_rank': country_rank,
        'visits_no': visits_no,
        'authority_score': authority_score,
        'pages_per_visit': pages_per_visit,
        'avg_visit_duration': avg_visit_duration,
        'bounce_rate': bounce_rate,
        'referring_domains': referring_domains,
        'backlinks': backlinks,
        'traffic_by_country': traffic_by_country,
        'traffic_by_device': traffic_by_device,
        'desktop_device': desktop_device,
        'mobile_device': mobile_device,
        'competitors': competitors,
        'organic_traffic': organic_traffic,
        'paid_traffic': paid_traffic,
        'categories': categories,
        'site_data': site_data
    }

    return {key: str(value) for key, value in result.items()}


# Read the CSV file containing domain names
sites_list = pd.read_csv('semrush_listed_sites_concurrent.csv')
domains = sites_list['domain_name']

site_column_names = [
    'domain_name', 'global_rank', 'country_rank', 'visits_no',
    'authority_score', 'pages_per_visit', 'avg_visit_duration',
    'bounce_rate', 'referring_domains', 'backlinks', 'traffic_by_country',
    'traffic_by_device', 'desktop_device', 'mobile_device',
    'competitors', 'organic_traffic', 'paid_traffic', 'categories', 'site_data'
]

# Function to process and save site data concurrently
def process_and_save_data():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_site_data, domain): domain for domain in domains}

        with open('site_data.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=site_column_names)
            writer.writeheader()

            for future in concurrent.futures.as_completed(futures):
                domain = futures[future]
                try:
                    site_result = future.result()
                    if site_result:
                        writer.writerow(site_result)
                except Exception as e:
                    print(f"Error processing {domain}: {e}")

# Run the data processing
process_and_save_data()
