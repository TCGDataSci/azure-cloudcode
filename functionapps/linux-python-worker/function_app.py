# azure imports 
import azure.functions as func

# tcgds imports
from tcgds.reporting import EmailExceptionHandler
from tcgds.jobs import JOBS_QUEUE_NAME, JOBS_QUEUE_CONN_STR_NAME


# other imports 
import time
import json
import requests
import pandas as pd 
import requests.adapters
from datetime import datetime
from contextlib import ExitStack
from sqlalchemy import create_engine
from urllib.parse import quote, unquote
  

# scrapers
from tcgds.scrapes.dks import dks_product_scrape, dks_location_scrape
from tcgds.scrapes.sbux import sbux_location_scrape, sbux_unionization_scrape
# from tcgds.scrapes.five import five_location_scrape, five_product_scrape  
# from tcgds.scrapes.bookingdotcom import bookingdotcom_location_scrape
# from tcgds.scrapes.chtr import chtr_zipcode_scrape

# apis
from tcgds.apis.similarweb.similarweb import Similarweb, MONTHLY_UPDATE_DAY as similarweb_mud
from tcgds.apis.sensortower.sensortower import Sensortower, MONTHLY_UPDATE_DAY as sensortower_mud
from tcgds.apis.whalewisdom import Whalewisdom

# app initializtion
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)






@app.queue_trigger('message', JOBS_QUEUE_NAME, JOBS_QUEUE_CONN_STR_NAME)
def job_orchestrator(message:func.QueueMessage):
    with ExitStack() as stack:
        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = 'Job Orchestrator'
        

        json_message = message.get_json()
        function_name = json_message['function_name']
        exc_handler.subject = json_message['job_name']

        # apis
        if function_name == 'sensortower_update':
            update_freqs = ['daily']
            if (tday:=datetime.today()).weekday()==0:
                update_freqs.append('weekly')
            if tday.day==sensortower_mud:
                update_freqs.append('monthly')
                if tday.month in [1, 4, 7, 10]:
                    update_freqs.append('quarterly')
        
            with Sensortower() as sens:
                for update_freq in update_freqs: 
                    for platform in ['unified', 'ios', 'android']:
                        groups = sens.get_update_params_groups(platform, update_freq) 
                        if groups is not None:
                            for params in groups.groups.keys():
                                first_group:pd.DataFrame = groups.get_group(params) 
                                update_args = {'app_ids':first_group['app_id'].to_list(),   
                                            'platform':platform} 
                                update_args.update(json.loads(params)) 
                                sens.update_data(**update_args) 


        if function_name == 'similarweb_update':
            update_freqs = ['daily']
            if (tday:=datetime.today()).weekday()==0:
                update_freqs.append('weekly')
            if tday.day==similarweb_mud:
                update_freqs.append('monthly')
                if tday.month in [1, 4, 7, 10]:
                    update_freqs.append('quarterly')
            with Similarweb() as simweb: 
                for update_freq in update_freqs: 
                    update_params_df = simweb.get_update_params(update_freq)
                    update_params_dict_lst = update_params_df.to_dict('records')
                    for elt in update_params_dict_lst[309:]:
                        simweb.update_data(elt['domain'], elt['data_type'], **json.loads(elt['update_params']))


        if function_name == '13f_update':
            with Whalewisdom() as whale:
                whale.update_holdings()

        # scrapes
        if function_name == 'dks_location_scrape':
            dks_location_scrape(json_message)

        if function_name == 'dks_product_scrape':
            dks_product_scrape(json_message)

        if function_name == 'sbux_location_scrape':
            sbux_location_scrape(json_message)

        if function_name == 'sbux_unionization_scrape':
            sbux_unionization_scrape(json_message)

        # if function_name == 'five_locaton_scrape':
        #     five_location_scrape(json_message)

        # if function_name == 'five_product_scrape':
        #     five_product_scrape(json_message)

        # if function_name == 'bookingdotcom_location_scrape':
        #     bookingdotcom_location_scrape(json_message)

        # if function_name == 'chtr_zipcode_scrape':
        #     chtr_zipcode_scrape(json_message)       