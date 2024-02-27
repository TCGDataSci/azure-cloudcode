# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
# built-in imports
import os
import json
import asyncio
from datetime import datetime
#third-party imports
import pandas as pd
# tcgds imports
from tcgds.apis.sensortower import Sensortower, MONTHLY_UPDATE_DAY as sensortower_mud
from tcgds.apis.similarweb import Similarweb, MONTHLY_UPDATE_DAY as simweb_mud
from tcgds.reporting import aioreport
from tcgds.apis.whalewisdom import *

## IMPORTANT: arg_name must be camlcase (even though the argument name is snake case)

KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
 
# psql constants (used by Scraper class)
os.environ['psql_username'] = secret_client.get_secret('PSQLUsername').value
os.environ['psql_password'] = secret_client.get_secret('PSQLPassword').value  

# initialize function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route("apis/sensortower/update")
async def sensortower_update(req:func.HttpRequest):
    update_freqs = ['daily']
    if (tday:=datetime.today()).weekday()==0:
        update_freqs.append('weekly')
    if tday.day==sensortower_mud:
        update_freqs.append('monthly')
        if tday.month in [1, 4, 7, 10]:
            update_freqs.append('quarterly')
 
    async with Sensortower() as sens:
        all_update_tasks = [] 
        for update_freq in update_freqs: 
            @aioreport('Sensortower update ' + update_freq)
            async def main_func(update_freq:str):
                tasks = []
                for platform in ['unified', 'ios', 'android']:
                    groups = sens.get_update_params_groups(platform, update_freq) 
                    if groups is not None:
                        for params in groups.groups.keys():
                            first_group:pd.DataFrame = groups.get_group(params) 
                            update_args = {'app_ids':first_group['app_id'].to_list(),   
                                        'platform':platform} 
                            update_args.update(json.loads(params)) 
                            tasks.append(sens.update_data(**update_args)) 
                await asyncio.gather(*tasks)
            all_update_tasks.append(main_func(update_freq))
        await asyncio.gather(*all_update_tasks) 
 

# similarweb data update function
@app.route("apis/similarweb/update")
async def similarweb_update(req:func.HttpRequest):
    update_freqs = ['daily','monthly']
    if (tday:=datetime.today()).weekday()==0: 
        update_freqs.append('weekly')
    if tday.day==simweb_mud: 
        update_freqs.append('monthly')
        if tday.month in [1, 4, 7, 10]: 
            update_freqs.append('quarterly')

    async with Similarweb() as simweb: 
        all_update_tasks = []
        for update_freq in update_freqs: 
            @aioreport('Similarweb update ' + update_freq)
            async def main_func(update_freq:str):
                update_params_df = simweb.get_update_params(update_freq)
                update_params_dict_lst = update_params_df.to_dict('records')
                update_tasks = [simweb.update_data(elt['domain'], elt['data_type'], **json.loads(elt['update_params'])) for elt in update_params_dict_lst]
                await asyncio.gather(*update_tasks)
            all_update_tasks.append(main_func(update_freq))
        await asyncio.gather(*all_update_tasks) 



SA_NAME = "maintcgdssa"
SA_URL = f"https://{SA_NAME}.blob.core.windows.net"
SA_KEY_NAME = f'{SA_NAME}Key'

# initializing via key vault and environment variables
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
client = SecretClient(vault_url=KVUrl, credential=credential)


whale_shared_key = client.get_secret('WhaleWisdomSharedKey').value
whale_secret_key = client.get_secret('WhaleWisdomSecretKey').value
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_key = client.get_secret(SA_KEY_NAME).value
blob_service_client = BlobServiceClient(SA_URL, credential=sa_key)
pg = Postgres(psql_username, psql_password, 'test')
whale = Whalewisdom(whale_shared_key, whale_secret_key, blob_service_client, pg)



@app.route("apis/whalewisdom/13fUpdate")
def whalewisdom_13fupdate(req:func.HttpRequest):
    whale.update_holdings()

@app.route("apis/whalewisdom/filerUpdate")
def whalewisdom_updatefilers(req:func.HttpRequest):
    whale.update_filers()