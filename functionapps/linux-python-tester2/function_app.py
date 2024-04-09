# others
import time
import requests
import requests.adapters
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text, select, insert

# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# tcgds imports
from tcgds.scrape import BlobResponseSaver, base_headers, PARSE_ERROR_PATH
from tcgds.postgres import psql_connection, PandasPGHelper
from tcgds.reporting import send_report



credential = EnvironmentCredential()
KVUrl = "https://tcgdsvault.vault.azure.net/"
client = SecretClient(vault_url=KVUrl, credential=credential)
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_connection_string = client.get_secret('mainStorageAccount').value
psql_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password))


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route('testing/bookscrape')
def bookscrape_scrape(req:func.HttpRequest):
    req_obj = req.get_json()
    pdpg_helper = PandasPGHelper()
    
    blob_path = req_obj['operation_name'] + '/' + req_obj['instance_id'] 
    with BlobResponseSaver(sa_connection_string, blob_path=blob_path) as blob_response_saver:
            # initialize requests session
        throttle = 8
        total_retries = 2 
        backoff_factor = 2
        status_forcelist = [400, 401, 404, 500]
        s = requests.Session()
        r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
        s.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
        s.hooks['response'].append(blob_response_saver.save_requests_response)
        s.headers = base_headers


        # scrape
        state = None
        for url in url_generator(state):
            time.sleep(throttle)
            try:
                custom_response = s.get(url)
                try:
                    data_df = parse(custom_response['resp_text'])
                    pdpg_helper.to_sql(data_df, 'catalogue_data', 'bookstoscrape', if_exists='replace')
                except:
                    blob_response_saver.save_custom_response(custom_response, blob_path+'/parse-errors')


            except:
                send_report('Failed')






def url_generator(state:list=None):
    if state is not None:
        urls = state
    else:
        start = 1
        num_pages = 10
        urls = [f"https://books.toscrape.com/catalogue/page-{num}.html" for num in range(start, num_pages)]
    while len(urls)>0:
        url = urls.pop()
        state = urls
        yield url


def parse(data:str):
    """
    Parses scraped data

    Args:
        data (Union[str, io.BytesIO, InputStream]): data to be parsed 
    """
    soup = BeautifulSoup(data, features='lxml')
    title_tags = soup.find_all('a', title=True)
    price_tags =  soup.find_all('div', attrs={'class':'product_price'})
    meta_data = []
    for title_tag, price_tag in list(zip(title_tags, price_tags)):
        data_dict = {}
        data_dict['title'] = title_tag.attrs['title']
        data_dict['href'] = title_tag.attrs['href']
        data_dict['price'] = price_tag.find('p', attrs={'class':'price_color'}).text[1:]
        data_dict['stock_availability'] = price_tag.find('p', attrs={'class':'instock availability'}).text.replace('  ', '').replace('\n', '')
        meta_data.append(data_dict) 
    return pd.DataFrame(meta_data)