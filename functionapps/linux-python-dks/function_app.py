# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# tcgds imports
from tcgds.scrapes.dks import *
from tcgds.postgres import PandasPGHelper, psql_connection

# other imports 
import os
import json
import uuid
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# psql constants     
os.environ['psql_username'] = psql_username = secret_client.get_secret('PSQLUsername').value  
os.environ['psql_password'] = psql_password = secret_client.get_secret('PSQLPassword').value

# app initializtion
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



@app.route("scrapes/dks/locations")
def dks_location_scrape(req:func.HttpRequest):
    scrape_guid = uuid.uuid4()
    date_today = datetime.today()
    location_df = get_dks_locations_sitemap()
    location_df['scrape_guid'] = [scrape_guid]*len(location_df)
    location_df['observed_date'] = [date_today]*len(location_df)
    pg_helper = PandasPGHelper(user=psql_username, password=psql_password)
    pg_helper.to_sql(location_df, 'store_locations', 'dks')



@app.route("scrapes/dks/products")
def dks_product_scrape(req:func.HttpRequest):
    # get search term from request body
    req_obj = json.load(req.get_body())
    search_term = req_obj['search_term']
    # get most recent location data
    pg_engine = create_engine(psql_connection.format(user=psql_username, password=psql_password))
    with pg_engine.connect() as pg_connection:
        query = ("select * from dks.store_locations where observed_date = (select max(observed_date) from dks.store_locations)")
        location_df = pd.read_sql_query(query, pg_connection)
    # get get all product and sku data for search term
    product_search_results = get_all_products_search(search_term, report=True)
    skus_meta_df, skus_attributes_df = get_skus_data(product_search_results['dsg_seo_url'].to_list(), return_attributes=True, report=True)
    skus_inventory_df = get_inventory_data(skus_meta_df['sku_id'].to_list(), location_df['store_code'].astype(str).to_list(), report=True)
    skus_inventory_df = skus_inventory_df.merge(skus_meta_df[['sku_id', 'product_id']], how='left', left_on='sku', right_on='sku_id').drop('sku', axis=1) 
    # add observed date
    date_today = datetime.today()
    product_search_results['observed_date'] = [date_today]*len(product_search_results)
    skus_meta_df['observed_date'] = [date_today]*len(skus_meta_df)
    skus_inventory_df['observed_date'] = [date_today]*len(skus_inventory_df)
    skus_attributes_df['observed_date'] = [date_today]*len(skus_attributes_df)
    # add scrape guid
    scrape_guid = uuid.uuid4()
    product_search_results['scrape_guid'] = [scrape_guid]*len(product_search_results)
    skus_meta_df['scrape_guid'] = [scrape_guid]*len(skus_meta_df)
    skus_inventory_df['scrape_guid'] = [scrape_guid]*len(skus_inventory_df)
    skus_attributes_df['scrape_guid'] = [scrape_guid]*len(skus_attributes_df)
    # push to postgres
    pg_helper = PandasPGHelper()
    pg_helper.to_sql(product_search_results, 'product_data', 'dks')
    pg_helper.to_sql(skus_meta_df, 'skus_meta_data', 'dks')
    pg_helper.to_sql(skus_attributes_df, 'skus_attributes_data', 'dks')
    pg_helper.to_sql(skus_inventory_df, 'skus_inventory_data', 'dks')