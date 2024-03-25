# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient    

# tcgds imports
from tcgds.scrapes.five import scrape_all_categories
from tcgds.scrape import base_headers, custom_format_response, SCRAPE_DATA_CONTAINER, SCRAPE_ERROR_CONTAINER, PARSE_ERROR_CONTAINER
from tcgds.postgres import PandasPGHelper
from tcgds.reporting import send_report

# other imports
import os
import json
import uuid
import time
import requests
import requests.adapters
import traceback
from datetime import datetime
from bs4 import BeautifulSoup



KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# psql constants     
os.environ['psql_username'] = psql_username = secret_client.get_secret('PSQLUsername').value  
os.environ['psql_password'] = psql_password = secret_client.get_secret('PSQLPassword').value
storage_connection_string = secret_client.get_secret('maintcgdssaConnectionString')


app = func.FunctionApp()

@app.route("scrapes/five/products")
def five_product_scrape(req:func.HttpRequest):
    scrape_all_categories(progress=False)


### five below loaction scrapes ###
# @app.route("scrapes/five/locations")
# def five_location_scrape(req:func.HttpRequest):
#     # scrape variables
#     scrape_guid = uuid.uuid4().hex
#     scrape_name = "location_scrape" # for saving responses
#     domain_name = "fivebelow.com" # for saving responses
#     blob_path_prefix = f'{domain_name}/{scrape_name}/{scrape_guid}/' # for saving blob files

#     # postgres variables
#     pg_table = "" # for parsing
#     pg_schema = "bookingdotcom" # for parsing
#     pdpg_helper = PandasPGHelper(user=psql_username, password=psql_password)

#     # initialize requests session
#     throttle = 8
#     total_retries = 2 
#     backoff_factor = 2
#     status_forcelist = [400, 401, 404, 500]
#     s = requests.Session()
#     r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
#     s.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
#     s.headers = base_headers

#     # scrape 1
#     def url_generator(state:list):
#         if len(state)==0:
#             location_sitemap_url = 'https://locations.fivebelow.com/sitemap.xml'
#             response = requests.get(location_sitemap_url, headers=base_headers)
#             soup = BeautifulSoup(response.text, features='lxml') 
#             locations = [elt.text for elt in soup.find_all('loc') if len(elt.text.split('/'))>5]
#             state = locations
#         while len(state)>0:
#             yield state.pop(0)


#     # scrape'
#     state = dict()
#     for url in url_generator(state):
#         time.sleep(throttle)
#         try:
#             response = s.get(url)
#             custom_response = custom_format_response(response, scrape_guid=scrape_guid)
#             blob_name = blob_path_prefix+custom_response.url_uuid+'-'+datetime.strptime(custom_response['datetime'], "%Y-%m-%d %H:%M:%S").timestamp()+'.txt'
#             with BlobServiceClient.from_connection_string(storage_connection_string) as blob_service_client:
#                 if (status_code:=response.status_code)>=200 and status_code<300:
#                     # mutli thread these sections
#                     blob_client = blob_service_client.get_blob_client(SCRAPE_DATA_CONTAINER, blob_name)
#                     blob_client.upload_blob(json.dumps(custom_response))

#                     # parse
#                     try:
#                         response_obj = custom_response['response']
#                         soup = BeautifulSoup(response_obj)
#                         # extracted_data = parser(soup)
#                         # pdpg_helper.to_sql(extracted_data, pg_table, pg_schema)
#                     except Exception as e:
#                         blob_client = blob_service_client.get_blob_client(PARSE_ERROR_CONTAINER, blob_name)
#                         blob_client.upload_blob(json.dumps(custom_response))

#                 elif status_code>=400 and status_code<500:
#                     blob_client = blob_service_client.get_blob_client(SCRAPE_ERROR_CONTAINER, blob_name)
#                     blob_client.upload_blob(json.dumps(custom_response))
#         except Exception as e:
#             send_report("Five below location scrape", traceback.format_exc().replace('\n', '<br>'))
#     return 'success'