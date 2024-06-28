# azure imports 
import azure.functions as func
import azure.durable_functions as df  

# tcgds imports
from tcgds.jobs import JOBS_QUEUE_NAME, JOBS_QUEUE_CONN_STR_NAME 
from tcgds.customauth import CustomAuth
from tcgds.postgres import psql_connection_string

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.queue_trigger('message', JOBS_QUEUE_NAME, JOBS_QUEUE_CONN_STR_NAME)
def analysis_job_orchestrator(message:func.QueueMessage):
    
    pass