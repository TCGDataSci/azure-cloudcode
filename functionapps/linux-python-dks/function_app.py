# azure imports 
import azure.functions as func
import azure.durable_functions as df  
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient

# tcgds imports
from tcgds.scrapes.dks import *
from tcgds.postgres import PandasPGHelper

# other imports 
import os
import uuid
from datetime import datetime

KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# psql constants     
os.environ['psql_username'] = psql_username = secret_client.get_secret('PSQLUsername').value  
os.environ['psql_password'] = psql_password = secret_client.get_secret('PSQLPassword').value
storage_connection_string = secret_client.get_secret('maintcgdssaConnectionString')

app = func.FunctionApp()



@app.rout("scrapes/dks/locations")
def dks_location_scrape(req:func.HttpRequest):
    scrape_guid = uuid.uuid4()
    location_df = get_dks_locations_sitemap()
    location_df['scrape_guid'] = [scrape_guid]*len(location_df)
    pg_helper = PandasPGHelper(user=psql_username, password=psql_password)
    pg_helper.to_sql(location_df, 'store_locations', 'dks')



@app.route("scrapes/dks/products")
def dks_product_scrape(req:func.HttpRequest):
    scrape_guid = uuid.uuid4()
    location_df = get_dks_locations_sitemap()
    product_search_results = get_all_products_search('on', report=True)
    skus_meta_df, skus_attributes_df = get_skus_data(product_search_results['dsg_seo_url'].to_list(), return_attributes=True, report=True)
    skus_inventory_df = get_inventory_data(skus_meta_df['sku_id'].to_list(), location_df['store_code'].astype(str).to_list(), report=True)
    skus_inventory_df = skus_inventory_df.merge(skus_meta_df[['sku_id', 'product_id']], how='left', left_on='sku', right_on='sku_id').drop('sku', axis=1)
    pg_helper = PandasPGHelper(user=psql_username, password=psql_password)
    date_today = datetime.today()
    location_df['observed_date'] = [date_today]*len(location_df)
    product_search_results['observed_date'] = [date_today]*len(product_search_results)
    skus_meta_df['observed_date'] = [date_today]*len(skus_meta_df)
    skus_inventory_df['observed_date'] = [date_today]*len(skus_inventory_df)
    skus_attributes_df['observed_date'] = [date_today]*len(skus_attributes_df)
    
    product_search_results['scrape_guid'] = [scrape_guid]*len(product_search_results)
    skus_meta_df['scrape_guid'] = [scrape_guid]*len(skus_meta_df)
    skus_inventory_df['scrape_guid'] = [scrape_guid]*len(skus_inventory_df)
    skus_attributes_df['scrape_guid'] = [scrape_guid]*len(skus_attributes_df)
    
    pg_helper.to_sql(product_search_results, 'product_data', 'dks')
    pg_helper.to_sql(skus_meta_df, 'skus_meta_data', 'dks')
    pg_helper.to_sql(skus_attributes_df, 'skus_attributes_data', 'dks')
    pg_helper.to_sql(skus_inventory_df, 'skus_inventory_data', 'dks')