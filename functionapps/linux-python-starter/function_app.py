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
def sensortower_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/sensortower/update")
    return response

# similarweb update
@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def similarweb_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/similarweb/update")
    return response

# whalewisdom update
@app.timer_triger('timer', '0 0 12,14,16,18,20 13-21 2,5,8,11 *' , run_on_startup=False)
def whalewisdom_update(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-apimanager.azurewebsites.net/api/apis/whalewisdom/13fupdate")
    return response



### SMALL SCRAPES ###

# dutchbros location scrape
@app.timer_trigger('timer', "0 0 17 * * 0", run_on_startup=False)
def dutchbros_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes.azurewebsites.net/api/scrapes/bros/locations")
    return response

# avdx sales person schedule scrape
@app.timer_trigger('timer', "0 0 8 * * 1-5", run_on_startup=False)
def avdx_sales_schedule_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes.azurewebsites.net/api/scrapes/avdx/salesSchedule") 
    return response

@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-small-scrapes.azurewebsites.net/api/scrapes/five/locations")
    return response 


### LARGE SCRAPES ###

# booking.com scrape
@app.timer_trigger('timer', '0 0 8 1 * *', run_on_startup=False)
def bookingdotcom_ushotels_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-bookingdotcom.azurewebsites.net/api/scrapes/bookingdotcom/ushotels")
    return response

# charter random address scrape
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def chtr_address_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-chtr.azurewebsites.net/api/scrapes/chtr/rando_addresses")
    return response

# charter zip code scrape
@app.timer_trigger('timer', '0 0 9 15 * *', run_on_startup=False)
def chtr_zipcode_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-chtr.azurewebsites.net/api/scrapes/chtr/zip_codes")
    return response

# five below product scrape
@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_product_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-five.azurewebsites.net/api/scrapes/five/products")
    return response 


# dks location scrape
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    response = requests.get("https://linux-python-dks.azurewebsites.net/api/scrapes/dks/locations")
    return response 


# dks product scrape ON
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    req_body = {'search_term':'on'}
    response = requests.get("https://linux-python-dks.azurewebsites.net/api/scrapes/dks/products", json=json.dumps(req_body))
    return response 


# dks product scrape HOKA
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_location_scrape(timer:func.TimerRequest):
    if timer.past_due:
        pass
    req_body = {'search_term':'hoka'}
    response = requests.get("https://linux-python-dks.azurewebsites.net/api/scrapes/dks/products", json=json.dumps(req_body))
    return response 