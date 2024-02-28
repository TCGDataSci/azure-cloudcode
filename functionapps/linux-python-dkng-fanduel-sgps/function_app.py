# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

# other imports
import json
import time
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# tcgds imports
from tcgds.scrapes.dkng import sport_groups_keys, get_events as dkng_get_events, get_event_pre_fabs, get_event_sgps as dkng_get_event_sgps
from tcgds.scrapes.fanduel import get_events as fanduel_get_events, get_event_sgps as fanduel_get_event_sgps  
from tcgds.postgres import Postgres, psql_connection


SA_NAME = "maintcgdssa"
SA_URL = f"https://{SA_NAME}.blob.core.windows.net"
SA_KEY_NAME = f'{SA_NAME}Key'
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
client = SecretClient(vault_url=KVUrl, credential=credential)

# psql constants
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_key = client.get_secret(SA_KEY_NAME).value


app = func.FunctionApp()

@app.timer_trigger(arg_name="timer", schedule="0 0 12 * * *", run_on_startup=False)
def dkng_sgp_queue_scrape(timer: func.TimerRequest):
    if timer.past_due:
        pass
    sa_connection_string = f"DefaultEndpointsProtocol=https;AccountName={SA_NAME};AccountKey={sa_key};EndpointSuffix=core.windows.net"
    queue = QueueClient.from_connection_string(sa_connection_string, "prod2queue")
    encoder = TextBase64EncodePolicy()

    dkng_pg = Postgres(psql_username, psql_password, 'dkng')
    dkng_event_data = dkng_get_events(sport_groups_keys['NFL Football']['eventGroupId'])
    now_plus_24 = datetime.utcnow() + relativedelta(hours=24) 
    dkng_next_24hrs = dkng_event_data.loc[dkng_event_data['startDate'] <= now_plus_24].reset_index(drop=True)
    if not dkng_next_24hrs.empty:
        with dkng_pg:
            dkng_pg.to_sql(dkng_next_24hrs, 'dkng_events_meta', 'added')
        dkng_event_time_groups = dkng_next_24hrs.groupby('startDate')
        dkng_event_times = dkng_event_time_groups.groups.keys()
        for event_time in dkng_event_times:
            dkng_event_ids = dkng_event_time_groups.get_group(event_time)['eventId'].to_list()
            msg_dict = {'func':'dkng', 'event_ids':dkng_event_ids}
            dkng_timeout = (event_time-datetime.utcnow()).seconds-420
            queue.send_message(encoder.encode(json.dumps(msg_dict)), visibility_timeout=dkng_timeout)  


@app.timer_trigger('timer', '0 0 12 * * *', run_on_startup=False)
def fanduel_sgp_queue_scrape(timer:func.TimerRequest):
    sa_connection_string = f"DefaultEndpointsProtocol=https;AccountName={SA_NAME};AccountKey={sa_key};EndpointSuffix=core.windows.net"
    queue = QueueClient.from_connection_string(sa_connection_string, "prod2queue")
    encoder = TextBase64EncodePolicy()

    fanduel_pg = Postgres(psql_username, psql_password, 'fanduel')
    event_data = fanduel_get_events()
    now_plus_24 = datetime.utcnow() + relativedelta(hours=24)
    next_24hrs = event_data.loc[event_data['openDate'] <= now_plus_24].reset_index(drop=True)
    if not next_24hrs.empty:
        with fanduel_pg:
            fanduel_pg.to_sql(next_24hrs, 'fanduel_events_meta', 'added')    
        event_time_groups = next_24hrs.groupby('openDate')
        event_times = event_time_groups.groups.keys()
        for event_time in event_times:
            fd_evnet_ids = event_time_groups.get_group(event_time)['eventId'].to_list()
            msg_dict = {'func':'fanduel', 'event_ids':fd_evnet_ids}
            fanduel_timeout = (event_time-datetime.utcnow()).seconds-180
            queue.send_message(encoder.encode(json.dumps(msg_dict)), visibility_timeout=fanduel_timeout)



@app.queue_trigger('azqueue', queue_name="prod2queue", connection="SA_CONNECTION_STRING")
def sgp_scrape(azqueue:func.QueueMessage):
    data_dict = json.loads(azqueue.get_body().decode('utf-8'))
    if data_dict['func'] == "fanduel":
        fanduel_pg = Postgres(psql_username, psql_password, 'fanduel')
        sgp_data = pd.DataFrame()
        for event_id in data_dict['event_ids']:
            time.sleep(2)
            event_sgp_data = fanduel_get_event_sgps(event_id=event_id)
            sgp_data = pd.concat([event_sgp_data, sgp_data])
        columns = ['type', 'bettingOpportunityId', 'totalBets', 'eventId', 'competitionId', 'selections', 'americanOdds', 'decimalOdds', 'parlay_legs']
        sgp_data = sgp_data[columns]
        with fanduel_pg:
            fanduel_pg.to_sql(sgp_data, 'fanduel_event_sgps', 'added')
    else:
        dkng_pg = Postgres(psql_username, psql_password, 'dkng')
        dkng_event_pre_fabs = get_event_pre_fabs(data_dict['event_ids'])
        dkng_event_sgps = dkng_get_event_sgps(dkng_event_pre_fabs)
        with dkng_pg:
            dkng_pg.to_sql(dkng_event_pre_fabs, 'dkng_event_pre_fab_bets', 'added')
            dkng_pg.to_sql(dkng_event_sgps, 'dkng_event_sgps', 'added')
