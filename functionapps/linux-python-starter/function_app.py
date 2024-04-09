# azure imports 
import azure.functions as func

# other imports
import json
import requests

# define functionapp
app = func.FunctionApp()



### APIS ###

# sensortower update  
@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def sensortower_update(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/sensortower/update")
    return response

# similarweb update
@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def similarweb_update(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/similarweb/update")
    return response

# whalewisdom update
@app.timer_trigger('timer', '0 0 12,14,16,18,20 13-21 2,5,8,11 *' , run_on_startup=False)
def whalewisdom_update(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/whalewisdom/13fupdate")
    return response






### SMALL SCRAPES ###

# dutchbros location scrape
@app.timer_trigger('timer', "0 0 17 * * 0", run_on_startup=False)
def dutchbros_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/bros/locations")
    return response

# avdx sales person schedule scrape
@app.timer_trigger('timer', "0 0 8 * * 1-5", run_on_startup=False)
def avdx_sales_schedule_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/avdx/salesSchedule") 
    return response

# five below locaton scrape
@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/five/locations")
    return response 

# fdic isider filing scrape
@app.timer_trigger('timer', '0 0 0 * * *', run_on_startup=False)
def fdic_insider_filing_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/fdic/insiderFilings")
    return response





### LARGE SCRAPES ###

# booking.com scrape
@app.timer_trigger('timer', '0 0 8 1 * *', run_on_startup=True)
def bookingdotcom_ushotels_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-bookingcom-tcgdatasci.azurewebsites.net/api/scrapes/bookingdotcom/ushotels")
    return response

# charter random address scrape
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def chtr_address_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-chtr-tcgdatasci.azurewebsites.net/api/scrapes/chtr/rando_addresses")
    return response

# charter zip code scrape
@app.timer_trigger('timer', '0 0 9 15 * *', run_on_startup=False)
def chtr_zipcode_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-chtr-tcgdatasci.azurewebsites.net/api/scrapes/chtr/zip_codes")
    return response

# five below product scrape
@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_product_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-five-tcgdatasci.azurewebsites.net/api/scrapes/five/products")
    return response 

# dks location scrape 
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/locations")
    return response 

# dks product scrape ON
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_on_product_scrape(timer:func.timer.TimerRequest):

    if timer.past_due:
        pass
    req_body = {'search_term':'on'}
    response = requests.post("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/products", json=req_body)
    return response 

# dks product scrape HOKA
@app.timer_trigger('timer', '0 0 11 1 * *', run_on_startup=False)
def dks_hoka_product_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    req_body = {'search_term':'hoka'}
    response = requests.post("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/products", json=req_body)
    return response 

from azure.keyvault.secrets import SecretClient
from azure.identity import EnvironmentCredential
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
from sqlalchemy import create_engine, text, select
from tcgds.postgres import psql_connection, Operations
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from croniter import croniter

credential = EnvironmentCredential()
encoder = TextBase64EncodePolicy()

KVUrl = "https://tcgdsvault.vault.azure.net/"
client = SecretClient(vault_url=KVUrl, credential=credential)
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_connection_string = client.get_secret('mainStorageAccount').value
psql_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password))


@app.timer_trigger('timer', "0 0 11,23 * * *", run_on_startup=False)
def queue_method_test(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    time_now = datetime.now()
    time_plus_12 = time_now + relativedelta(hours=12)
    # get scheduled operations
    with psql_engine.connect() as psql_connection:
        q = select(Operations.__table__)
        q_results = psql_connection.execute(q).all()
        results_df = pd.DataFrame(q_results)
    # add operations within 12 hours to queues
    queue_client = QueueClient.from_connection_string(sa_connection_string, 'operations-queue')
    for row in results_df.iterrows():
        cron_expr = row[1]['cron_schedule']
        if croniter.match_range(cron_expr, time_now, time_plus_12):
            message = row[1].to_dict()
            encoded_message = encoder.encode(json.dumps(message))
            queuetime = (croniter(cron_expr).get_next(datetime) - croniter(cron_expr).get_current(datetime)).total_seconds()
            queue_client.send_message(encoded_message, visibility_timeout=queuetime)
    queue_client.close()