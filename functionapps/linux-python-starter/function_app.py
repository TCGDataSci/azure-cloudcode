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
    requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/sensortower/update")
    

# similarweb update
@app.timer_trigger('timer', '0 0 11 * * *', run_on_startup=False)
def similarweb_update(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/similarweb/update")
    

# whalewisdom update
@app.timer_trigger('timer', '0 0 12,14,16,18,20 13-21 2,5,8,11 *' , run_on_startup=False)
def whalewisdom_update(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-apimanager-tcgdatasci.azurewebsites.net/api/apis/whalewisdom/13fupdate")
    






### SMALL SCRAPES ###

# dutchbros location scrape
@app.timer_trigger('timer', "0 0 17 * * 0", run_on_startup=False)
def dutchbros_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/bros/locations")
    

# avdx sales person schedule scrape
@app.timer_trigger('timer', "0 0 8 * * 1-5", run_on_startup=False)
def avdx_sales_schedule_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/avdx/salesSchedule") 
    

# five below locaton scrape
@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/five/locations")
     

# fdic isider filing scrape
@app.timer_trigger('timer', '0 0 0 * * *', run_on_startup=False)
def fdic_insider_filing_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-small-scrapes-tcgdatasci.azurewebsites.net/api/scrapes/fdic/insiderFilings")
    





### LARGE SCRAPES ###

# booking.com scrape
@app.timer_trigger('timer', '0 0 8 1 * *', run_on_startup=True)
def bookingdotcom_ushotels_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-bookingcom-tcgdatasci.azurewebsites.net/api/scrapes/bookingdotcom/ushotels")
    

# charter random address scrape
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def chtr_address_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-chtr-tcgdatasci.azurewebsites.net/api/scrapes/chtr/rando_addresses")
    

# charter zip code scrape
@app.timer_trigger('timer', '0 0 9 15 * *', run_on_startup=False)
def chtr_zipcode_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-chtr-tcgdatasci.azurewebsites.net/api/scrapes/chtr/zip_codes")
    

# five below product scrape
@app.timer_trigger('timer', '0 0 9 * * 0', run_on_startup=False)
def five_product_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-five-tcgdatasci.azurewebsites.net/api/scrapes/five/products")
     

# dks location scrape 
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_location_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    requests.get("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/locations")
     

# dks product scrape ON
@app.timer_trigger('timer', '0 0 9 1 * *', run_on_startup=False)
def dks_on_product_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    req_body = {'search_term':'on'}
    requests.post("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/products", json=req_body)
    

# dks product scrape HOKA
@app.timer_trigger('timer', '0 0 11 1 * *', run_on_startup=False)
def dks_hoka_product_scrape(timer:func.timer.TimerRequest):
    if timer.past_due:
        pass
    req_body = {'search_term':'hoka'}
    requests.post("https://linux-python-dks-tcgdatasci.azurewebsites.net/api/scrapes/dks/products", json=req_body)
    

