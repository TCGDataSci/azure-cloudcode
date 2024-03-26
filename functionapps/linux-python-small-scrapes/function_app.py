# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# tcgds import
from tcgds.scrapes.bros import location_scrape as bros_location_scrape
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
    # blob_container = 'scrape-data-container'
    # blob_storage_path = 'dutchbros.com/locations_scrape/'
    try:
        # presets for scraping
        loc_url = "https://files.dutchbros.com/api-cache/stands.json"
        headers= base_headers
        headers['accept-encoding'] = ''
        # make get request to json file
        response = requests.get(loc_url, headers=headers)
        # blob_name = uuid.uuid3(uuid.NAMESPACE_URL, loc_url).hex + str(datetime.now().timestamp()).replace(".", "") + '.txt' 
        # with BlobServiceClient.from_connection_string(os.environ['SA_CONNECTION_STRING']) as blob_service_client:
        #     blob_client = blob_service_client.get_blob_client(blob_container, blob_storage_path + blob_name)
        #     blob_client.upload_blob(response.text)
        json_obj = json.loads(response.text)        

        # to postgres
        pg_schema = 'bros'
        pg_engine = create_engine(psql_connection.format(user=os.environ['psql_username'], password=os.environ['psql_password']))
        df = pd.DataFrame(json_obj)   
        df.to_sql('store_locations', pg_engine, schema=pg_schema, if_exists='append', index=False, method='multi')

    except:
        send_report("Dutchbros location scrape", traceback.format_exc().replace('\n', '<br>'))





### five below loaction scrapes ###
from tcgds.scrapes.five import extract_location_data, location_url_generator

@app.route("scrapes/five/locations")
def five_location_scrape(req:func.HttpRequest):
    import time 
    extracted_data = []
    throttle = 3
    for url in location_url_generator(): 
        headers = base_headers.copy()
        headers['Referer'] = 'https://locations.fivebelow.com/coming-soon.html'
        headers['Accept'] = 'application/json'
        response = requests.get(url, headers=headers)
        json_obj = response.json()
        entities = json_obj['response']['entities']
        extracted_data.extend([extract_location_data(entity) for entity in entities])
        time.sleep(throttle)
    data_df = pd.DataFrame(extracted_data)
    pg_helper = PandasPGHelper(user=psql_username, password=psql_password)
    pg_helper.to_sql(data_df, table_name='store_locations_1', schema='five_stuff', if_exists='replace')    


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



### fdic insider filing scrape
from tcgds.scrapes.fdic import FDIC
@app.route('scrapes/fdic/insiderFilings')
def fdic_insider_filings_scrape(req:func.HttpRequest):
    fdick = FDIC(psql_username, psql_password)
    fdick.scrape_new_disclosures()