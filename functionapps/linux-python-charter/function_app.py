# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# other imports
import os
import time
import uuid
import random
import asyncio  
import traceback
import pandas as pd
from sqlalchemy import create_engine, text

# tcgds imports
from tcgds.postgres import PandasPGHelper, psql_connection
from tcgds.scrapes.chtr import *
from tcgds.scrape import json_format_response, blob_upload
from tcgds.reporting import send_report


## IMPORTANT: all wrapper argument names must be camlcase  s
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# psql constants     
psql_username = secret_client.get_secret('PSQLUsername').value  
psql_password = secret_client.get_secret('PSQLPassword').value    
os.environ['psql_username'] = psql_username
os.environ['psql_password'] = psql_password


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# zip code scrape
@app.route("scrapes/chtr/zip_codes")
def chtr_zip_code_check(req:func.TimerRequest):
    pg_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password)) 
    zip_codes = pd.read_sql_table('zip_codes', pg_engine, schema='chtr')
    for zip_code in zip_codes['zip_codes']:
        time.sleep(random.choice(range(3,6))) 
        data = check_zipcode(zip_code)
        data_df = pd.DataFrame([data])
        PandasPGHelper().to_sql(data_df, 'zip_code_data', 'chtr') 


# random address scrape
@app.route("scrapes/chtr/rando_addresses")
async def chtr_rando_addresses(req:func.TimerRequest):
    throttle = 5 
    allowed_consecutive_400s = 20
    scrape_guid = uuid.uuid4().hex
    try:
        pg_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password))
        with pg_engine.connect() as connection:
            stmt = (
                "select * from chtr.rando_addresses as ra "
                "join (select distinct zipcode from chtr.zip_code_data where \"serviceVendorName\" != \'none\') as zip "
                "on ra.postcode=zip.zipcode"
            )
            rando_addresses = list(connection.execute(text(stmt)).all())
            consecutive_400 = 0
            for address in rando_addresses:    
                try:
                    response = check_address(address, timeout=30)
                except TypeError:
                    continue
                json_response = json_format_response(response, scrape_guid=scrape_guid)
                address_str = ''
                for elt in address[:-1]:
                    if elt is not None:
                        address_str = address_str+'-'+elt
                blob_name = f'spectrum.com/random_addresses_scrape/{scrape_guid}/' + json_response['url_uuid'] + address_str.lower().replace(' ','-')
                parsed = False
                parsed_datetime = None
                if (status_code:=json_response['status_code']) >= 401 and status_code<500:
                    await blob_upload(json_response, 'scrape-error-container', blob_name, os.getenv('SA_CONNECTION_STRING'), tags={'parsed':str(parsed), 'parsed_datetime':str(parsed_datetime)}, add_timestamp=True)
                    raise Exception(f'{status_code} status code')
                elif status_code == 400:
                    consecutive_400 += 1
                    if consecutive_400 >= allowed_consecutive_400s:
                        raise Exception(f'{consecutive_400} consecutive {status_code} status codes')
                    else:
                        continue
                elif status_code >= 200 and status_code < 300:
                    consecutive_400 = 0
                    json_response.update({'line1':address[1]+address[2], 'line2':address[3], 'city':address[4], 'state':address[0], 'zipcode':address[5]})                    
                    await blob_upload(json_response, 'scrape-data-container', blob_name, os.getenv('SA_CONNECTION_STRING'), tags={'parsed':str(parsed), 'parsed_datetime':str(parsed_datetime)}, add_timestamp=True)
                asyncio.sleep(throttle)
    except Exception as e:            
        send_report("Spectrum Random Address Error", traceback.format_exc().replace('\n', '<br>') + f'<br> Last Address Index: {rando_addresses.index(address)}<br> Address: {address_str[1:].lower().replace(" ","-")}')
        raise e
