# others
import json
from io import BytesIO

# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# tcgds imports
import tcgds


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route('testing/bookscrape')
def bookscrape_scrape(req:func.HttpRequest):
    req_obj = req.get_json()
    operation_id = req_obj['operation_id']
    instance_id = req_obj['instance_id']
