# azure imports 
import azure.functions as func

# tcgds imports
from tcgds.auth import Auth
from tcgds.reporting import EmailExceptionHandler
from tcgds.utils import to_snake, find_dict_key_kw
from tcgds.scrape import Scraper, Step, base_headers
from tcgds.postgres import PandasPGHelper, psql_connection_string

# other imports 
import gzip
import time
import json
import math
import requests
import pandas as pd 
from io import BytesIO
import requests.adapters
from bs4 import BeautifulSoup
from urllib.parse import quote
from contextlib import ExitStack
from urllib.parse import unquote
from datetime import datetime, UTC
from sqlalchemy import create_engine
  

# app initializtion
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



@app.route(route="scrapes/dks/locations", auth_level=func.AuthLevel.ANONYMOUS)
def dks_location_scrape(req:func.HttpRequest):
    with ExitStack() as stack:
        # initialize exception handler
        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = 'Bookstoscrape Exception'

        # parse request object
        req_obj:dict = req.get_json()
        job_name = req_obj['job_name']
        instance_id = req_obj['instance_id']
        req_body = req_obj.get('body')
        if req_body is not None:
            pass

        # initialize scraper
        scraper = stack.enter_context(Scraper(job_name=job_name, instance_id=instance_id))
        if not scraper.initialized:    
            init_step = Step(location_sitemap_url, 0)
            scraper.init_route(init_step)

        # initialize requests session
        throttle = 2
        total_retries = 2 
        backoff_factor = 2
        status_forcelist = [400, 401, 404, 500]
        r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
        req_session = stack.enter_context(requests.Session())
        req_session.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
        req_session.hooks['response'].append(scraper.response_handler)
        req_session.headers = base_headers


        # scrape
        for url, level in scraper.route:
            response = requests.get(url)

            if level==0:
                parsed_data = dks_location_sitemap_parser(response)
                next_steps = [Step(store_availability_url+str(store_code), 1) for store_code in pd.DataFrame(parsed_data)['store_code']]
                scraper.route.add(next_steps)
                scraper.data['location_data'] = parsed_data
            elif level==1:
                parsed_data = dks_store_availability_parser(response)
                scraper.data['availability_data'].append(parsed_data)
            # throttle
            time.sleep(throttle)

        # upload data to postgres
        location_df = pd.DataFrame(scraper.data['location_data'])
        availability_df = pd.DataFrame(scraper.data['availability_data'])
        availability_df['store_code'] = availability_df['store_code'].astype(int)
        full_location_df = location_df.merge(availability_df, how='left', on='store_code')
        full_location_df.drop(['storeHours', 'curbsideHours'], axis=1, inplace=True)
        full_location_df.columns = [to_snake(col) for col in full_location_df.columns]
        full_location_df['instance_id'] = [instance_id]*len(full_location_df)
        full_location_df['observed_datetime'] = [datetime.now(tz=UTC)]*len(full_location_df)
        pdpg_helper = PandasPGHelper()
        affected_rows = pdpg_helper.to_sql(full_location_df, 'store_locations', 'dks', affected_rows=True)





@app.route(route="scrapes/dks/products", auth_level=func.AuthLevel.ANONYMOUS)
def dks_product_scrape(req:func.HttpRequest):
    with ExitStack() as stack:
        # initialize exception handler
        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = 'DKS Product Scrape'

        # initialize auth object
        auth = stack.enter_context(Auth(environment='virtual'))
        psql_username, psql_password = auth.get_postgres_credentials()
        pg_engine = create_engine(psql_connection_string.format(user=psql_username, password=psql_password))
        with pg_engine.connect() as pg_connection:
            query = ("select * from dks.store_locations where observed_datetime = (select max(observed_datetime) from dks.store_locations)")
            location_df = pd.read_sql_query(query, pg_connection)


        # parse request object
        req_obj:dict = req.get_json()
        job_name = req_obj['job_name']
        instance_id = req_obj['instance_id']
        req_body = req_obj.get('body')
        if req_body is not None:
            search_term = req_body['search_term']

        # initialize scraper
        scraper = stack.enter_context(Scraper(job_name=job_name, instance_id=instance_id))
        if not scraper.initialized:    
            init_steps = [Step(create_product_search_query(search_term, store_code), 0) for store_code in location_df['store_code']]
            scraper.init_route(init_steps)

        # initialize requests session
        throttle = 5
        total_retries = 2 
        backoff_factor = 2
        status_forcelist = [400, 401, 404, 500]
        r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
        req_session = stack.enter_context(requests.Session())
        req_session.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
        req_session.hooks['response'].append(scraper.response_handler)
        req_session.headers = base_headers

        # init PandasPGHelper
        pdpg_helper = PandasPGHelper()

        for url, level in scraper.route:

            if level!=2:
                response = requests.get(url)
            

            if level == 0:
                # first page of product search at given store
                url_data = json.loads(unquote(url).split('=')[1])
                store_code = url_data['selectedStore']
                page_num = url_data['pageNumber']
                if page_num==0:
                    product_search_data, num_pages = dks_product_search_page_parser(response, first_page=True)
                    store_code = int(json.loads(unquote(url).split('=')[1])['selectedStore'])
                    next_search_pages = [Step(create_product_search_query(search_term, store_code, page_num), 0) for page_num in range(1, num_pages)]
                    scraper.route.add(next_search_pages, place='front')
                else:
                    product_search_data = dks_product_search_page_parser(response)
                next_product_pages = [Step(home_url+asset_href, 1) for asset_href in pd.DataFrame(product_search_data)['dsg_seo_url']]
                scraper.route.add(next_product_pages, 'front')

                # upload product search data to postgres
                product_search_df = pd.DataFrame(product_search_data)
                product_search_df['observed_datetime'] = [datetime.now(tz=UTC)]*len(product_search_df)
                product_search_df['instance_id'] = [instance_id]*len(product_search_df)
                affected_rows = pdpg_helper.to_sql(product_search_df, 'product_search_data', 'dks', affected_rows=True)

            elif level == 1:
                # product sku data from product page
                skus_data, attributes_data = dks_product_page_parser(response)  
                # add inventory urls to route
                inventory_queries = create_inventory_urls(pd.DataFrame(skus_data)['sku_id'].to_list(), location_df['store_code'].astype(str).to_list())
                inventory_steps = [Step(inventory_query, 2) for inventory_query in inventory_queries]
                scraper.route.add(inventory_steps)
                # uplaod sku attribute data to postgres
                attributes_df = pd.DataFrame(attributes_data)
                attributes_df['observed_datetime'] = [datetime.now(tz=UTC)]*len(attributes_df)
                attributes_df['instance_id'] = [instance_id]*len(attributes_df)
                affected_rows = pdpg_helper.to_sql(attributes_df, 'skus_attributes_data', 'dks', affected_rows=True)
                # append skus data to scraper data object
                scraper.data['skus_data'].extend(skus_data)


            elif level == 2:
                # sku inventory data 
                x_api_key = "pdp-90c1c1ae-1580-11ec-a613-4f5255dd8e74"
                headers = base_headers.copy()
                headers['X-Api-Key'] = x_api_key
                response = requests.get(url, headers=headers)
                inventory_data = dks_inventory_data_parser(response)
                scraper.data['inventory_data'].extend(inventory_data)

            # throttle requests
            time.sleep(throttle)
        

        # combine merge product id and sku id from meta df
        skus_meta_df = pd.DataFrame(scraper.data['skus_data'])
        inventory_df = pd.DataFrame(scraper.data['inventory_data'])
        inventory_merged_df = inventory_df.merge(skus_meta_df[['sku_id', 'product_id']], how='left', left_on='sku', right_on='sku_id').drop('sku', axis=1) 
        skus_meta_df['observed_datetime'] = [datetime.now(tz=UTC)]*len(skus_meta_df)
        inventory_merged_df['observed_datetime'] = [datetime.now(tz=UTC)]*len(inventory_merged_df)
        skus_meta_df['instance_id'] = [instance_id]*len(skus_meta_df)
        inventory_merged_df['instance_id'] = [instance_id]*len(inventory_merged_df)            
        pdpg_helper.to_sql(skus_meta_df, 'skus_meta_data', 'dks')
        pdpg_helper.to_sql(inventory_merged_df, 'skus_inventory_data', 'dks')







 
# home url
home_url = "https://www.dickssportinggoods.com"

# location urls
location_sitemap_url = "https://stores.dickssportinggoods.com/sitemap.xml.gz"
storelocator_url = "https://storelocator.dickssportinggoods.com/responsive/ajax?"
store_availability_url =  "https://availability.dickssportinggoods.com/api/v3/stores/"

# product search url
product_search_url =  "https://prod-catalog-product-api.dickssportinggoods.com/v2/search?searchVO="

# inventory url
inventory_url = "https://availability.dickssportinggoods.com/v2/inventoryapis/searchinventory?"

# inventory api key
x_api_key = "pdp-90c1c1ae-1580-11ec-a613-4f5255dd8e74"



# product search query dict
query_dict = {
    "pageNumber":0,
    "pageSize":48,
    "selectedStore":"",
    "selectedFilters":{
        "facetStore":["BOPIS"],
    },
    "selectedSort":0,
    "snbAudience":"",
    "preview":"",
    "appliedSeoFilters":False,
    "searchTerm":"on",
    "storeId":"",
    "zipcode":""
}



def dks_location_sitemap_parser(response:requests.Response):
    with gzip.open(BytesIO(response.content)) as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, features='xml')
    loc_tags = soup.find_all('loc')
    urls = [elt for loc_tag in loc_tags if len((elt:=loc_tag.contents[0]).split('/'))==7]
    location_dict = dict()
    location_dict['url'] = urls
    location_dict['store_code'] = [int(url.split('/')[5]) for url in urls]
    return pd.DataFrame(location_dict).to_json(orient='records')


def dks_store_availability_parser(response:requests.Response):
    return response.json()



def create_product_search_query(search_term:str, store_code:int=0, page_number:int=0):
    query_dict['selectedStore'] = str(store_code)
    query_dict['searchTerm'] = search_term
    query_dict['pageNumber'] = page_number
    search_url = product_search_url + quote(json.dumps(query_dict))
    return search_url



def extract_attributes(product_list:list, new_attribute_names:bool=False):
    attribute_mapping = {"5495":"gender", "5382":"product_type", "4298":"features_overlay_detailed", "2101":"gender_by_age", "X_BRAND":"brand", "5525":"features_overlay", "PRIMARY_CATEGORY_DSG ":"primary_category", "4187":"product_type_1"}
    for product in product_list:
        attribute_dict = dict()
        for attribute in json.loads(product['attributes']):
            try:
                attribute_dict.update(attribute)
            except:
                print(attribute)
                return
        for key in attribute_mapping.keys():
            attribute_value = attribute_dict.get(key, None)
            product[attribute_mapping[key]] = attribute_value
    if new_attribute_names:
        return list(attribute_mapping.values())


def dks_product_search_page_parser(response:requests.Response, first_page:bool=False):
    # get json result
    result = response.json()
    if first_page:
        total_products = result['totalCount']
        page_size = query_dict['pageSize']
        num_pages = math.ceil(total_products/page_size)
    product_vos_lst:list = result['productVOs']
    product_details_dict:dict = result['productDetails']
        
    # product vos
    new_attribute_names = extract_attributes(product_vos_lst, new_attribute_names=True)
    keep_cols = ['catentryId', 'name', 'mfName', 'badge', 'assetSeoUrl', 'dsgSeoUrl', 'dsgProductSortDate', 'partnumber'] + new_attribute_names
    product_vos_df:pd.DataFrame = pd.DataFrame(product_vos_lst)[keep_cols]
    
    # product details
    catentryIds = list(product_details_dict.keys())
    product_details_df = pd.json_normalize(list(product_details_dict.values())) 
    product_details_df['catentryId'] = catentryIds
    product_details_df['catentryId'] = product_details_df['catentryId'].astype(int)
    product_search_df = product_vos_df.merge(product_details_df, how='left', on='catentryId')
    product_search_df.columns = [to_snake(col_name.replace('.', '_')) for col_name in product_search_df.columns]
    product_search_df.rename(columns={'partnumber':'product_id'}, inplace=True)
    if first_page:
        total_products = result['totalCount']
        page_size = query_dict['pageSize']
        num_pages = math.ceil(total_products/page_size)
        return product_search_df.to_json(orient='records'), num_pages
    else:
        return product_search_df.to_json(orient='records')


def dks_product_page_parser(response:requests.Response):
    soup = BeautifulSoup(response.text, features="lxml")
    json_obj = json.loads(soup.find_all('script', attrs={'id':'dcsg-ngx-pdp-server-state'})[0].text)
    product = json_obj[find_dict_key_kw(json_obj, 'product-')]
    product_id = product['id']
    skus_df = pd.json_normalize(product['skus'])
    skus_df['product_id'] = [product_id]*skus_df.shape[0]
    skus_df.columns = [to_snake(col_name.replace('.', '_')) for col_name in skus_df.columns]
    skus_df.rename(columns={'id':'sku_id'}, inplace=True)
    skus_df.drop('desc_attributes', axis=1, inplace=True)
    
    # get attributes 
    sku_attributes_lst = list(zip(skus_df['sku_id'], skus_df['def_attributes']))
    skus_attributes_df = pd.DataFrame()
    for sku_id, attributes in sku_attributes_lst:
        attribute_df = pd.DataFrame(attributes)
        attribute_df['sku_id'] = [sku_id]*attribute_df.shape[0]
        skus_attributes_df = pd.concat([skus_attributes_df, attribute_df], ignore_index=True)
    skus_df.drop('def_attributes', axis=1, inplace=True)
    return skus_df.to_json(orient='records'), skus_attributes_df.to_json(orient='records')   


def create_inventory_urls(sku_ids:list, store_codes:list, batch_size:int=50):
    inventory_querys = [inventory_url+"location="+",".join(store_codes)+"&sku="+",".join(sku_ids[i:i+batch_size]) for i in range(0, len(sku_ids), batch_size)]
    return inventory_querys


def dks_inventory_data_parser(response:requests.Response):
    return response.json()['data']['skus']