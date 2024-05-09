# azure imports 
import azure.functions as func
import azure.durable_functions as durfunc

# tcgds imports
from tcgds.auth import Auth
from tcgds.reporting import EmailExceptionHandler
from tcgds.postgres import psql_connection_string
from tcgds.utils import to_snake
from tcgds.scrape import Scraper, Step, base_headers
from tcgds.jobs import JOBS_QUEUE


# other imports 
import time
import json
import requests
import pandas as pd 
import requests.adapters
from contextlib import ExitStack
from sqlalchemy import create_engine
from urllib.parse import quote, unquote
  

# scrapers
from tcgds.scrapes.dks import dks_product_scrape, dks_location_scrape
from tcgds.scrapes.sbux import sbux_location_scrape, sbux_unioniztion_scrape
# from tcgds.scrapes.five import five_location_scrape, five_product_scrape
# from tcgds.scrapes.bookingdotcom import bookingdotcom_location_scrape
# from tcgds.scrapes.chtr import chtr_zipcode_scrape

# app initializtion
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)






@app.queue_trigger('message', JOBS_QUEUE, 'JOB_QUEUE_CONNECTION_STRING')
def job_orchestrator(message:func.QueueMessage):
    with ExitStack() as stack:
        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = 'Job Orchestrator'
        


        json_message = message.get_json()
        function_name = json_message['function_name']


        # apis
        if function_name == 'sensortower_update':
            pass

        if function_name == 'similarweb_update':
            pass


        if function_name == 'dks_location_scrape':
            dks_location_scrape(json_message)

        if function_name == 'dks_product_scrape':
            dks_product_scrape(json_message)

        if function_name == 'sbux_location_scrape':
            sbux_location_scrape(json_message)

        if function_name == 'sbux_unionization_scrape':
            sbux_unioniztion_scrape(json_message)

        # if function_name == 'five_locaton_scrape':
        #     five_location_scrape(json_message)

        # if function_name == 'five_product_scrape':
        #     five_product_scrape(json_message)

        # if function_name == 'bookingdotcom_location_scrape':
        #     bookingdotcom_location_scrape(json_message)

        # if function_name == 'chtr_zipcode_scrape':
        #     chtr_zipcode_scrape(json_message)       