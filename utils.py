import json
from bs4 import BeautifulSoup
import requests

def extract_json_from_text(cat_soup):
    """
    Extracts JSON data embedded in a specific script tag from an HTML document.

    This function searches through the `<script>` tags in a BeautifulSoup object
    to find one that contains a JavaScript variable `window.__PRELOADED_STATE__`.
    It then extracts the JSON data assigned to this variable.

    Args:
        cat_soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content.

    Returns:
        dict or None: Returns a dictionary containing the JSON data if extraction
        and parsing are successful. Returns `None` if the script tag or JSON data
        cannot be found or if there is an error during JSON decoding.

    Example:
        ```python
        from bs4 import BeautifulSoup

        html_doc = '''
        <html><head><script>window.__PRELOADED_STATE__ = {"key": "value"};</script></head><body></body></html>
        '''
        soup = BeautifulSoup(html_doc, 'html.parser')
        data = extract_json_from_text(soup)
        print(data)  # Output: {'key': 'value'}
        ```

    Notes:
        - The function looks for script tags containing `window.__PRELOADED_STATE__`
          followed by a JSON string and attempts to decode the JSON.
        - If the function encounters a JSON decoding error, it prints the error and returns `None`.
        - Make sure the input HTML is parsed using BeautifulSoup before passing it to the function.
    """
    cat_scripts = cat_soup.find_all('script')
    search_str = 'window.__PRELOADED_STATE__ = '
    cat_search_str_len = len(search_str)

    for script in cat_scripts:
        if script.text.strip().startswith(search_str):
            text = script.text.strip()
            cat_json_str = text[cat_search_str_len:-1]

            try:
                cat_json_data = json.loads(cat_json_str)
                return cat_json_data
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                return None
    print("Could not find the required script tag.")
    return None


def extract_json(json_str):
    """
    Parses a JSON string and returns the corresponding Python dictionary.
    """
    try:
        json_data = json.loads(json_str)
        return json_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def fetch_trending_website_data(url, headers):
    """
    Fetches and parses trending website data from a given URL.

    This function retrieves HTML content from a specified URL, searches for a specific 
    script tag containing a JavaScript variable `window.__PRELOADED_STATE__` with JSON data, 
    and parses it into a Python dictionary. It also extracts relevant column names for data processing.

    Args:
        url (str): The URL to fetch data from.
        headers (dict): HTTP headers to use for the GET request.

    Returns:
        tuple: A tuple containing:
            - list : Categories extracted from semrush url.
            - list: Column names extracted from the JSON data to create empty DataFrame.

    Example:
        ```python
        url = 'https://www.semrush.com/trending-websites/us/all'
        headers = {'User-Agent': 'your-user-agent'}
        categories, column_names = fetch_trending_website_data(url, headers)
        ```

    Notes:
        - The function searches for the script tag containing `window.__PRELOADED_STATE__` and extracts the JSON data.
    """
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    search_str = 'window.__PRELOADED_STATE__ = '
    search_str_len = len(search_str)

    scripts = soup.find_all('script')
    
    # Get index of the script tag containing 'window.__PRELOADED_STATE__ = '
    scripts_text_index = next(
        (index for index, script in enumerate(scripts) if script.text.strip().startswith(search_str)),
        None
    )
    
    if scripts_text_index is None:
        print("Could not find the required script tag.")
        return None, []

    text = scripts[scripts_text_index].text.strip()
    json_str = text[search_str_len:-1]

    # Parse the JSON data
    try:
        json_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None, []

    # Extract column names
    if 'data' in json_data and 'domains' in json_data['data'] and json_data['data']['domains']:
        column_names = list(json_data['data']['domains'][0].keys())
        column_names.append('category')
    else:
        column_names = []

    return json_data['categories'], column_names