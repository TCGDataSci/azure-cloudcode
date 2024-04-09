# azure imports 
import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import EnvironmentCredential
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

# tcgds imports
from tcgds.postgres import psql_connection, Operations, Instances
from tcgds.reporting import send_report

# other
import uuid
import json
import requests
import traceback
import pandas as pd
from croniter import croniter
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text, select, insert




credential = EnvironmentCredential()
encoder = TextBase64EncodePolicy()

KVUrl = "https://tcgdsvault.vault.azure.net/"
client = SecretClient(vault_url=KVUrl, credential=credential)
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_connection_string = client.get_secret('mainStorageAccount').value
psql_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password))


app = func.FunctionApp()

OPS_QUEUE = 'operations-queue'

@app.timer_trigger('timer', "0 0 11,23 * * *", run_on_startup=False)
def timer_queue_creation(timer:func.timer.TimerRequest):
    try:
        # initiate connections
        with psql_engine.connect() as psql_connection: 
            with QueueClient.from_connection_string(sa_connection_string, OPS_QUEUE) as queue_client:
                if timer.past_due:
                    pass
                # set times
                time_now = datetime.now()
                time_plus_12 = time_now + relativedelta(hours=12)


                try:
                    # get operations meta data
                    q = select(Operations.__table__)
                    q_results = psql_connection.execute(q).all()
                    results_df = pd.DataFrame(q_results)
                except:
                    send_report('Failed to get Operations table from postgres', traceback.format_exc().replace('\n', '<br>'))

                # add operations within 12 hours to queues
                for row in results_df.iterrows():
                    cron_expr = row[1]['cron_schedule']
                        # add to operations queue 
                    if croniter.match_range(cron_expr, time_now, time_plus_12):
                        try:
                            message = row[1].to_dict()
                            message['instance_id'] = uuid.uuid4().hex # create instance id for operation execution
                            encoded_message = encoder.encode(json.dumps(message))
                            queuetime = ((start_time:=croniter(cron_expr).get_next(datetime)) - croniter(cron_expr).get_current(datetime)).total_seconds()
                            queue_client.send_message(encoded_message, visibility_timeout=queuetime)
                        except:
                            send_report(f'Failed to queue operation {message['operation_name']}', traceback.format_exc().replace('\n','<br>'))
                    # add instance to instances table in postgres
                    try:
                        status = 'queued'
                        stmt = (
                            insert(Instances.__table__).
                            values(instance_id=message['instance_id'], operation_id=message['operation_id'], status=status, start_time=start_time)
                        )
                        psql_connection.execute(stmt)
                    except:
                        send_report(f'Failed to update Instances table for operation {message['operation_name']}', traceback.format_exc().replace('\n', '<br>'))
    except:
        send_report('Something failed in queueing operations', traceback.format_exc().replace('\n', '<br>'))



@app.queue_trigger('queue', OPS_QUEUE)
def queue_handler(queue:func.QueueMessage):
    try:
        message = json.loads(queue.get_body().decode('utf-8'))
        request_body = {'operation_name':message['operation_name'], 'operation_id':message['operation_id'], 'instance_id':message['instance_id']}
        if message['body'] is not None:
            request_body.update(message['body'])
        requests.request(message['request_method'], message['endpoint'], json=request_body)
    except:
        if 'message' in locals():
            send_report(f'Failed to start operation {message['operation_name']} with instance_id {message['instance_id']}', traceback.format_exc().replace('\n', '<br>'))
        else:
            send_report(f'Failed to start operation', traceback.format_exc().replace('\n', '<br>'))