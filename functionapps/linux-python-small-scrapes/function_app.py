# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# tcgds import
from tcgds.scrapes.bros import location_scrape as bros_location_scrape
from tcgds.scrapes.five import JsonLocationUrls 
from tcgds.reporting import report, send_report
from tcgds.scrape import base_headers, SCRAPE_DATA_CONTAINER, SCRAPE_ERROR_CONTAINER, PARSE_ERROR_CONTAINER, custom_format_response
from tcgds.postgres import psql_connection, Postgres, PandasPGHelper
from tcgds.scrapes.avdx import avdxSalesScheduleScape

# other imports
import os
import json
import uuid
import time
import requests
import requests.adapters
import traceback
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine


## IMPORTANT: all wrapper argument names must be camlcase  s
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# psql constants     
psql_username = secret_client.get_secret('PSQLUsername').value  
psql_password = secret_client.get_secret('PSQLPassword').value
storage_connection_string = secret_client.get_secret('maintcgdssaConnectionString')
os.environ['psql_username'] = psql_username
os.environ['psql_password'] = psql_password


app = func.FunctionApp()


### dutch bros location scrape ###
@app.route("scrapes/bros/locations")
def bros_location_scrape(req:func.HttpRequest):
    blob_container = 'scrape-data-container'
    blob_storage_path = 'dutchbros.com/locations_scrape/'
    try:
        # presets for scraping
        loc_url = "https://files.dutchbros.com/api-cache/stands.json"
        headers= base_headers
        headers['accept-encoding'] = ''
        # make get request to json file
        response = requests.get(loc_url, headers=headers)
        blob_name = uuid.uuid3(uuid.NAMESPACE_URL, loc_url).hex + str(datetime.now().timestamp()).replace(".", "") + '.txt' 
        with BlobServiceClient.from_connection_string(os.environ['SA_CONNECTION_STG']) as blob_service_client:
            blob_client = blob_service_client.get_blob_client(blob_container, blob_storage_path + blob_name)
            blob_client.upload_blob(response.text)
        json_obj = json.loads(response.text)        

        # to postgres
        pg_schema = 'bros'
        pg_engine = create_engine(psql_connection.format(user=os.environ['psql_username'], password=os.environ['psql_password']))
        df = pd.DataFrame(json_obj)   
        df.to_sql('store_locations', pg_engine, schema=pg_schema, if_exists='append', index=False, method='multi')

    except:
        send_report("Dutchbros location scrape", traceback.format_exc().replace('\n', '<br>'))





### five below loaction scrapes ###
@app.route("scrapes/five/locations")
def five_location_scrape(req:func.HttpRequest):
    # scrape variables
    scrape_guid = uuid.uuid4().hex
    scrape_name = "location_scrape" # for saving responses
    domain_name = "fivebelow.com" # for saving responses
    blob_path_prefix = f'{domain_name}/{scrape_name}/{scrape_guid}/' # for saving blob files

    # postgres variables
    pg_table = "" # for parsing
    pg_schema = "bookingdotcom" # for parsing
    pdpg_helper = PandasPGHelper(user=psql_username, password=psql_password)

    # initialize requests session
    throttle = 8
    total_retries = 2 
    backoff_factor = 2
    status_forcelist = [400, 401, 404, 500]
    s = requests.Session()
    r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    s.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
    s.headers = base_headers

    # scrape 1
    def url_generator(state:list):
        if len(state)==0:
            location_sitemap_url = 'https://locations.fivebelow.com/sitemap.xml'
            response = requests.get(location_sitemap_url, headers=base_headers)
            soup = BeautifulSoup(response.text, features='lxml') 
            locations = [elt.text for elt in soup.find_all('loc') if len(elt.text.split('/'))>5]
            state = locations
        while len(state)>0:
            yield state.pop(0)


    # scrape
    state = dict()
    for url in url_generator(state):
        time.sleep(throttle)
        try:
            response = s.get(url)
            custom_response = custom_format_response(response, scrape_guid=scrape_guid)
            blob_name = blob_path_prefix+custom_response.url_uuid+'-'+datetime.strptime(custom_response['datetime'], "%Y-%m-%d %H:%M:%S").timestamp()+'.txt'
            with BlobServiceClient.from_connection_string(storage_connection_string) as blob_service_client:
                if (status_code:=response.status_code)>=200 and status_code<300:
                    # mutli thread these sections
                    blob_client = blob_service_client.get_blob_client(SCRAPE_DATA_CONTAINER, blob_name)
                    blob_client.upload_blob(json.dumps(custom_response))

                    # parse
                    try:
                        response_obj = custom_response['response']
                        soup = BeautifulSoup(response_obj)
                        extracted_data = parser(soup)
                        pdpg_helper.to_sql(extracted_data, pg_table, pg_schema)
                    except Exception as e:
                        blob_client = blob_service_client.get_blob_client(PARSE_ERROR_CONTAINER, blob_name)
                        blob_client.upload_blob(json.dumps(custom_response))

                elif status_code>=400 and status_code<500:
                    blob_client = blob_service_client.get_blob_client(SCRAPE_ERROR_CONTAINER, blob_name)
                    blob_client.upload_blob(json.dumps(custom_response))
                    pass
        except Exception as e:
            pass
    return 'success'





### avdx sales schedule scrape ### 
@app.route("scrapes/avdx/salesSchedule")
def avdx_sales_schedule_scrape(req:func.HttpRequest):
    sales_sched_data = avdxSalesScheduleScape()
    data_df = pd.DataFrame(sales_sched_data)
    data_df['calendar_date'] = [datetime.strptime(c_date, "%Y-%m-%d") for c_date in data_df.calendar_date]
    data_df['calendar_available_times'] = data_df['calendar_available_times'].apply(json.dumps)
    pg = Postgres(psql_username, psql_password, 'avdx')
    with pg:
        pg.to_sql(data_df, 'sales_rep_schedule', 'added')

