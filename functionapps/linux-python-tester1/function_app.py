# azure imports 
import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import EnvironmentCredential
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

# tcgds imports
from tcgds.reporting import send_email_report, ExceptionHandler, pandas_to_html_col_foramtter
from tcgds.postgres import psql_connection_string
from tcgds.jobs import Job, Instance


# other
import uuid
import json
import requests
import pandas as pd
from croniter import croniter
from datetime import datetime, UTC
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, select, insert
from contextlib import ExitStack




credential = EnvironmentCredential()
encoder = TextBase64EncodePolicy()

KVUrl = "https://tcgdsvault.vault.azure.net/"
client = SecretClient(vault_url=KVUrl, credential=credential)
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_connection_string = client.get_secret('mainStorageAccount').value
psql_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))


app = func.FunctionApp()

JOBS_QUEUE = 'jobs-queue'

@app.timer_trigger('timer', "0 0 11,23 * * *", run_on_startup=False)
def queue_jobs(timer:func.timer.TimerRequest):
    # initiate connections
    with ExitStack() as stack:
        exc_handler = stack.enter_context(ExceptionHandler())
        exc_handler.subject_contexts.append('Job Queueing Exception:')
        psql_connection_string = stack.enter_context(psql_engine.connect())
        queue_client = stack.enter_context(QueueClient.from_connection_string(sa_connection_string, JOBS_QUEUE))

        if timer.past_due:
            pass
        # set times
        time_now = datetime.now(tz=UTC)
        twelve_hours_later = time_now + relativedelta(hours=12)


        exc_handler.subject = 'Querying Job from Postgres'
        # get Job meta data
        q = select(Job)
        q_results = psql_connection_string.execute(q).all()
        results_df = pd.DataFrame(q_results)
        exc_handler.subject = None

        # add Job within 12 hours to queues
        for row in results_df.iterrows():
            cron_expr = row[1]['cron_schedule']
                # add to Job queue 
            if croniter.match_range(cron_expr, time_now, twelve_hours_later):
                message = row[1].to_dict()
                exc_handler.subject = f'Failed to queue job {message['job_name']}'
                message['instance_id'] = uuid.uuid4().hex # create instance id for operation execution
                encoded_message = encoder.encode(json.dumps(message))
                queuetime = ((start_time:=croniter(cron_expr).get_next(datetime)) - croniter(cron_expr).get_current(datetime)).total_seconds()
                queue_client.send_message(encoded_message, visibility_timeout=queuetime)
                exc_handler.subject = None
            # add instance to Instance table in postgres
            exc_handler.subject = f'Failed to update Instance table for operation {message['job_name']}'
            status = 'queued'
            stmt = (
                insert(Instance).
                values(instance_id=message['instance_id'], job_id=message['job_id'], status=status, start_time=start_time)
            )
            psql_connection_string.execute(stmt)
            exc_handler.subject = None



@app.timer_trigger('timer', '0 15 11,23 * *', run_on_startup=False)
def send_daily_instance_report(timer:func.TimerRequest):
    pg_engine = create_engine(psql_connection_string.format(user=psql_username, passwrod=psql_password))
    with ExitStack() as stack:
        exc_handler = stack.enter_context(ExceptionHandler())
        exc_handler.subject = "Daily Instance Report Exception:"

        if timer.past_due:
            pass
        stati_lst = ['queued', 'running', 'failed', 'completed']

        pg_connection = stack.enter_context(pg_engine.connect())
        for status in stati_lst:            
            if status=="completed" or status=="failed":
                twelve_hrs_ago = (datetime.now(tz=UTC) - relativedelta(hours=12)) 
                query = select(Job.job_name, Job.job_id, Instance.instance_id, Instance.status, Instance.start_time).join(Job, Instance.job_id==Job.job_id).where(Instance.status==status, Instance.end_time>=twelve_hrs_ago) 
            else:
                query = select(Job.job_name, Job.job_id, Instance.instance_id, Instance.status, Instance.start_time).join(Job, Instance.job_id==Job.job_id).where(Instance.status==status)
            results = pd.DataFrame(pg_connection.execute(query).all())
            email_body = f'{status.capitalize()}:<br>'
            email_body+=results.to_html(index=False, formatters=[pandas_to_html_col_foramtter]*results.shape[1])
            email_body+='<br>'
        send_email_report('Daily Instance Report', email_body)




@app.queue_trigger('queue', JOBS_QUEUE)
def queue_handler(queue:func.QueueMessage):
    with ExceptionHandler() as exc_handler:
        exc_handler.subject_contexts.append('Job Queue Handler Exception:')
        message = json.loads(queue.get_body().decode('utf-8'))
        exc_handler.subject = f'Failed to start job {message['job_name']} with instance_id {message['instance_id']}'
        request_body = {'job_name':message['job_name'], 'job_id':message['job_id'], 'instance_id':message['instance_id']}
        if message['body'] is not None:
            request_body.update(message['body'])
        requests.request(message['request_method'], message['endpoint'], json=request_body)