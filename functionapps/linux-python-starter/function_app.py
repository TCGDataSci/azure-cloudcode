# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# tcgds imports
import tcgds

# other imports
import requests


app = func.FunctionApp()

### APIS ###
@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def sensortower_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/sensortower/update")
    return response


@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def similarweb_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/similarweb/update")
    return response


@app.timer_triger('timer', '0 0 12,14,16,18,20 13-21 2,5,8,11 *' , run_on_startup=False)
def whalewisdom_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/whalewisdom/13fupdate")
    return response

### SMALL SCRAPES ###
@app.timer_trigger('timer', "0 0 17 * * 0", run_on_startup=False)
def dutchbros_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes.azurewebsites.net/api/scrapes/bros/locations")
    return response


@app.timer_trigger('timer', "0 0 8 * * 1-5", run_on_startup=False)
def avdx_sales_schedule_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes.azurewebsites.net/api/scrapes/avdx/salesSchedule") 
    return response



### LARGE SCRAPES ###
# @app.timer_trigger('timer', '', run_on_startup=False)
# def bookingdotcom_ushotels_scrape(timer:func.TimerRequest):
#     if timer.past_due:
#         pass
#     response = requests.get("https://linux-python-bookingdotcom.azurewebsites.net/api/scrapes/bookingdotcom/ushotels")
#     return response


# @app.timer_trigger('timer', '', run_on_startup=False)
# def chtr_address_scrape(timer:func.TimerRequest):
#     if timer.past_due:
#         pass
#     response = requests.get("https://linux-python-chtr.azurewebsites.net/api/scrapes/chtr/rando_addresses")
#     return response


# @app.timer_trigger('timer', '', run_on_startup=False)
# def chtr_zipcode_scrape(timer:func.TimerRequest):
#     if timer.past_due:
#         pass
#     response = requests.get("https://linux-python-chtr.azurewebsites.net/api/scrapes/chtr/zip_codes")
#     return response


