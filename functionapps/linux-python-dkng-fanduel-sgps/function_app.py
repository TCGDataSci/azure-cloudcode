# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

# other imports
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

# tcgds imports
from tcgds.scrapes.dkng import *
from tcgds.postgres import Postgres


SA_NAME = "maintcgdssa"
SA_URL = f"https://{SA_NAME}.blob.core.windows.net"
SA_KEY_NAME = f'{SA_NAME}Key'
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
client = SecretClient(vault_url=KVUrl, credential=credential)

# psql constants
psql_connection = 'postgresql://{user}:{password}@azdspgsql1.postgres.database.azure.com:5432/postgres'
psql_username = client.get_secret('PSQLUsername').value
psql_password = client.get_secret('PSQLPassword').value
sa_key = client.get_secret(SA_KEY_NAME).value


app = func.FunctionApp()

@app.timer_trigger(arg_name="aztimer", schedule="0 0 12 * * *", run_on_startup=False)
def dkng_sgp_scrape(aztimer: func.TimerRequest):
    if aztimer.past_due:
        pass
    sa_connection_string = f"DefaultEndpointsProtocol=https;AccountName={SA_NAME};AccountKey={sa_key};EndpointSuffix=core.windows.net"
    queue = QueueClient.from_connection_string(sa_connection_string, "prod2queue")
    encoder = TextBase64EncodePolicy()

    dkng_pg = Postgres(psql_username, psql_password, 'dkng')
    dkng_event_data = get_events(sport_groups_keys['NFL Football']['eventGroupId'])
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