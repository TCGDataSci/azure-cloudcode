# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

# tcgds imports
from tcgds.reporting import send_email_report, EmailExceptionHandler, pandas_to_html_col_foramtter
from tcgds.postgres import psql_connection_string
from tcgds.jobs import Job, Instance, JOBS_QUEUE_NAME, JOBS_QUEUE_CONN_STR_NAME


# other
import os
import uuid
import json
import pandas as pd
from croniter import croniter
from datetime import datetime, UTC
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, select, insert
from contextlib import ExitStack


azure_key_vault_string = "https://{vault_name}.vault.azure.net/"
TCGDS_KEY_VAULT = "TCGDSVault"
az_credential = EnvironmentCredential()
secret_client = SecretClient(azure_key_vault_string.format(vault_name=TCGDS_KEY_VAULT), credential=az_credential)

psql_username = secret_client.get_secret('PSQLUsername').value  
psql_password = secret_client.get_secret('PSQLPassword').value



app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



# queue jobs via timer trigger runs every 12 hours
@app.timer_trigger('timer', "0 0 11,23 * * *", run_on_startup=False)
def queue_jobs(timer:func.TimerRequest):
    # initiate connections
    with ExitStack() as stack:
        exc_handler = stack.enter_context(EmailExceptionHandler())
        base_subject = 'Job Queueing Exception:'
        exc_handler.subject = base_subject
        psql_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
        psql_connection = stack.enter_context(psql_engine.connect())
        queue_client = stack.enter_context(QueueClient.from_connection_string(os.environ[JOBS_QUEUE_CONN_STR_NAME], JOBS_QUEUE_NAME))
        if timer.past_due:
            pass
        # set times
        time_now = datetime.now(tz=UTC)
        twelve_hours_later = time_now + relativedelta(hours=12)


        exc_handler.subject = base_subject + 'Querying Job from Postgres'
        # get Job meta data
        q_results = psql_connection.execute((select(Job).where(Job.status=='active'))).all()

        results_df = pd.DataFrame(q_results)
        exc_handler.subject = base_subject

        # add Job within 12 hours to queues
        for row in results_df.iterrows():
            cron_expr = row[1]['cron_schedule']
                # add to Job queue 
            if croniter.match_range(cron_expr, time_now, twelve_hours_later):
                message = row[1].to_dict()
                job_name = message.pop('name')
                message['job_name'] = job_name
                exc_handler.subject = base_subject + f'Failed to queue job {job_name}'
                message['instance_id'] = uuid.uuid4().hex # create instance id for operation execution
                encoder = TextBase64EncodePolicy()
                encoded_message = encoder.encode(json.dumps(message))
                queuetime = ((start_time:=croniter(cron_expr).get_next(datetime)) - croniter(cron_expr).get_current(datetime)).total_seconds()
                queue_client.send_message(encoded_message, visibility_timeout=queuetime)
                exc_handler.subject = base_subject
                # add instance to Instance table in postgres
                exc_handler.subject = base_subject + f'Failed to update Instance table for operation {job_name}'
                stmt = insert(Instance).values(id=message['instance_id'], job_id=message['id'], status='queued', start_time=start_time)
                psql_connection.execute(stmt)
                psql_connection.commit()
                exc_handler.subject = base_subject



# queue jobs via http endpoint
@app.route('job/queue/{jobname}')
def http_queue_jobs(req:func.HttpRequest):
    # initiate connections
    try:
        with ExitStack() as stack:
            exc_handler = stack.enter_context(EmailExceptionHandler())
            base_subject = 'Job Queueing Exception:'
            exc_handler.subject = base_subject
            psql_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
            psql_connection = stack.enter_context(psql_engine.connect())
            queue_client = stack.enter_context(QueueClient.from_connection_string(os.environ[JOBS_QUEUE_CONN_STR_NAME], JOBS_QUEUE_NAME))

            # add job to queue
            # add job to queue
            job_name:str = req.route_params.get('jobname')
            cron_start_time= req.get_json()['start_time']
            result = psql_connection.execute(select(Job).where(Job.name==job_name)).first()
            message = result._asdict()
            if cron_start_time is not None:
                message['cron_schedule'] = cron_start_time
            job_name = message.pop('name')
            message['job_name'] = job_name
            exc_handler.subject = base_subject + f'Failed to queue job {job_name}'
            message['instance_id'] = uuid.uuid4().hex # create instance id for operation execution
            encoder = TextBase64EncodePolicy()
            encoded_message = encoder.encode(json.dumps(message))
            queuetime = ((start_time:=croniter(message['cron_schedule']).get_next(datetime)) - croniter(message['cron_schedule']).get_current(datetime)).total_seconds()
            queue_client.send_message(encoded_message, visibility_timeout=queuetime)
            exc_handler.subject = base_subject
        
            # add instance to Instance table in postgres
            exc_handler.subject = base_subject + f'Failed to update Instance table for operation {job_name}'
            stmt = (
                insert(Instance).
                values(id=message['instance_id'], job_id=message['id'], status='queued', start_time=start_time)
            )
            psql_connection.execute(stmt)
            psql_connection.commit()
            exc_handler.subject = base_subject
        return func.HttpResponse(json.dumps(message), status_code=200)
    except Exception:
        return func.HttpResponse(f'Failed to queue job: {job_name}', status_code=500)



# run job via http endpoint
@app.route('job/run/{jobname}')
def http_run_jobs(req:func.HttpRequest):
    # initiate connections
    try:
        with ExitStack() as stack:
            exc_handler = stack.enter_context(EmailExceptionHandler())
            exc_handler.subject = 'Job Run Exception'
            psql_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
            psql_connection = stack.enter_context(psql_engine.connect())
            queue_client = stack.enter_context(QueueClient.from_connection_string(os.environ[JOBS_QUEUE_CONN_STR_NAME], JOBS_QUEUE_NAME))
            # create queue message
            job_name:str = req.route_params.get('jobname')
            result = psql_connection.execute(select(Job).where(Job.name==job_name)).first()
            message = result._asdict()
            job_name = message.pop('name')
            message['job_name'] = job_name
            message['instance_id'] = uuid.uuid4().hex # create instance id for operation execution

            # add instance to Instance table in postgres
            stmt = (
                insert(Instance).
                values(id=message['instance_id'], job_id=message['id'], status='queued')
            )
            psql_connection.execute(stmt)
            psql_connection.commit()
            # add message to queue
            encoder = TextBase64EncodePolicy()
            encoded_message = encoder.encode(json.dumps(message))
            queue_client.send_message(encoded_message)
        return func.HttpResponse(json.dumps(message), status_code=200)
    except Exception:
        return func.HttpResponse(f'{job_name} failed to run', status_code=500)



# restart job instance via http
@app.route('instance/restart/{instanceid}')
def http_restart_instance(req:func.HttpRequest):
    try:
        with ExitStack() as stack:
            exc_handler = stack.enter_context(EmailExceptionHandler())
            exc_handler.subject = 'Instance Restarting Exception'
            psql_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
            psql_connection = stack.enter_context(psql_engine.connect())


            instance_id:str = req.route_params.get('instanceid')
            job_id = psql_connection.execute(select(Instance.job_id).where(Instance.id==instance_id)).first()[0]
            q_results = psql_connection.execute(select(Job).where(Job.id == job_id)).first()
            message = q_results._asdict()
            job_name = message.pop('name')
            message['job_name'] = job_name
            message['instance_id'] = instance_id

            encoder = TextBase64EncodePolicy()
            encoded_message = encoder.encode(json.dumps(message))

            queue_client = stack.enter_context(QueueClient.from_connection_string(os.environ[JOBS_QUEUE_CONN_STR_NAME], JOBS_QUEUE_NAME))
            queue_client.send_message(encoded_message)
        return func.HttpResponse(json.dumps(message), status_code=200)
    except Exception:
        return func.HttpResponse(f'Failed to restart instance with instance_id: {instance_id}', status_code=500)




# send daily job report 
@app.timer_trigger('timer', '0 15 11,23 * * *', run_on_startup=False)
def send_daily_instance_report(timer:func.TimerRequest):
    with ExitStack() as stack:

        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = "Daily Instance Report Exception:"

        pg_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
        pg_connection = stack.enter_context(pg_engine.connect())

        stati_lst = ['queued', 'running', 'failed', 'completed']
        email_body = ''
        for status in stati_lst:            
            if status=="completed" or status=="failed":
                twelve_hrs_ago = (datetime.now(tz=UTC) - relativedelta(hours=12)) 
                query = select(Job.name, Job.id, Instance.id, Instance.status, Instance.start_time, Instance.end_time, Instance.machine).join(Job, Instance.job_id==Job.id).where(Instance.status==status, Instance.end_time>=twelve_hrs_ago) 
            else:
                query = select(Job.name, Job.id, Instance.id, Instance.status, Instance.start_time, Instance.end_time, Instance.machine).join(Job, Instance.job_id==Job.id).where(Instance.status==status) 
            results = pd.DataFrame(pg_connection.execute(query).all())
            if not results.empty:
                results.rename(columns={'id_1':'instance_id'}, inplace=True)
                if status=='completed' or status=='failed':
                    results['elapsed_time'] = results['end_time'] - results['start_time']
                email_body+=f'{status.capitalize()}:<br>'
                email_body+=results.to_html(index=False, formatters=[pandas_to_html_col_foramtter]*results.shape[1])
                email_body+='<br>'
        send_email_report('Daily Instance Report', email_body)