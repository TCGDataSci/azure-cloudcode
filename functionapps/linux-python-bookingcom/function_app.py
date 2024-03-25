# azure imports 
import azure.functions as func
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# other imports
import os
import uuid
import json
import time
import gzip
import requests
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup, element
import requests.adapters
import requests.exceptions
from datetime import datetime, timedelta

# tcgds imports
from tcgds.scrape import base_headers, TooManyConsecutiveSkips, TooManyTotalSkips, RetryException, custom_format_response, SCRAPE_DATA_CONTAINER, SCRAPE_ERROR_CONTAINER, PARSE_ERROR_CONTAINER
from tcgds.utils import to_snake, search_list_soup, string_replacements
from tcgds.postgres import PandasPGHelper


# authenticating with azure
KVUrl = "https://tcgdsvault.vault.azure.net/"
credential = EnvironmentCredential()
secret_client = SecretClient(vault_url=KVUrl, credential=credential)
  
# get secrets from azure key vault    
os.environ['psql_username'] = psql_username = secret_client.get_secret('PSQLUsername').value  
os.environ['psql_password'] = psql_password = secret_client.get_secret('PSQLPassword').value
storage_connection_string = secret_client.get_secret('maintcgdssaConnectionString')


# initialize functionapp
app = func.FunctionApp()

@app.route("scrapes/bookingdotcom/ushotels")
async def bookingdotcom_ushotel_scrape(req:func.HttpRequest):
    # scrape variables
    scrape_guid = uuid.uuid4().hex
    scrape_name = "us_hotel_scrape" # for saving responses
    domain_name = "booking.com" # for saving responses
    blob_path_prefix = f'{domain_name}/{scrape_name}/{scrape_guid}/' # for saving blob files
    
    # postgres variables
    pg_table = "us_hotels" # for parsing
    pg_schema = "bookingdotcom" # for parsing
    pdpg_helper = PandasPGHelper(user=psql_username, password=psql_password)

    # initialize requests session
    throttle = 8
    total_retries = 2 
    backoff_factor = 2
    status_forcelist = [400, 401, 404, 500]
    s = requests.Session()
    r = requests.adapters.Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    s.mount('https://', requests.adapters.HTTPAdapter(max_retries=r)) 
    s.headers = base_headers


    # scrape
    state = dict()
    for url in url_generator(state):
        time.sleep(throttle)
        try:
            response = s.get(url)
            custom_response = custom_format_response(response, scrape_guid=scrape_guid)
            blob_name = blob_path_prefix+custom_response.url_uuid+'-'+datetime.strptime(custom_response['datetime'], "%Y-%m-%d %H:%M:%S").timestamp()+'.txt'
            with BlobServiceClient.from_connection_string(storage_connection_string) as blob_service_client:
                if (status_code:=response.status_code)>=200 and status_code<300:
                    # mutli thread these sections
                    blob_client = blob_service_client.get_blob_client(SCRAPE_DATA_CONTAINER, blob_name)
                    blob_client.upload_blob(json.dumps(custom_response))

                    # parse
                    try:
                        response_obj = custom_response['response']
                        soup = BeautifulSoup(response_obj)
                        extracted_data = get_hotel_page_data(soup)
                        pdpg_helper.to_sql(extracted_data, pg_table, pg_schema)
                    except Exception as e:
                        blob_client = blob_service_client.get_blob_client(PARSE_ERROR_CONTAINER, blob_name)
                        blob_client.upload_blob(json.dumps(custom_response))

                elif status_code>=400 and status_code<500:
                    blob_client = blob_service_client.get_blob_client(SCRAPE_ERROR_CONTAINER, blob_name)
                    blob_client.upload_blob(json.dumps(custom_response))
                    pass
        except Exception as e:
            pass
    return 'success'


def url_generator(state:dict=dict(), start_num:int=46):
    # checks for initial state and yields until finished
    if len(state)!=0:
        start_num = int(state['page'])+1
        while len(init_urls:=state['urls_left'])>0:
            state = {'page':start_num-1, 'urls_left':init_urls} # save current state in case of failure again 
            yield init_urls.pop() + f"?checkin={(check_in:=datetime.today()).strftime('%Y-%m-%d')};checkout={(check_in+timedelta(1)).strftime('%Y-%m-%d')}"
    # iterates over remaining pages to yield new urls
    num_lst = list(range(start_num, 52))
    for num in num_lst:
        sitemap_url = f"https://www.booking.com/sitembk-hotel-en-us.{str(num).zfill(4)}.xml.gz"
        response = requests.get(sitemap_url, headers=base_headers)
        with gzip.open(BytesIO(response.content)) as file:
            file_content = file.read()
            soup = BeautifulSoup(file_content, features='xml')
            loc_tags = soup.find_all('loc')
            urls = [elt for loc_tag in loc_tags if (elt:=loc_tag.contents[0]).split('/')[4]=='us']
        while len(urls)>0:
            state = {'page':num, 'urls_left':urls} # save current state on each iteration
            yield urls.pop() + f"?checkin={(check_in:=datetime.today()+timedelta(1)).strftime('%Y-%m-%d')};checkout={(check_in+timedelta(1)).strftime('%Y-%m-%d')}"


def get_hotel_page_data(soup:BeautifulSoup) -> pd.DataFrame:
    """Returns dataframe of hotel meta data, sustainability data, category reviews and room data (includes price)"""
    return_df = lazy_hotel_meta_data(soup)
    more_data = hotel_more_meta_data(soup)
    return_df = pd.concat([return_df, more_data], axis=1)
    sstnblty = find_sustainable_tag_val(soup)
    return_df['sustainability'] = [sstnblty]*len(return_df)
    catreviews = get_catagory_reviews(soup)
    return_df['category_reviews'] = [catreviews]*len(return_df)
    return_df.drop('context', axis=1, inplace=True)
    return_df['room_meta_data'] = [get_room_meta_data(soup)]*len(return_df)
    for column in ['best_rating', 'rating_value', 'review_count']:
        if not column in return_df.columns:
            return_df[column] = [None]*len(return_df)
    return return_df


def lazy_hotel_meta_data(soup:BeautifulSoup) -> pd.DataFrame:
    json_tag = soup.find('script', attrs={'type':'application/ld+json'})
    json_obj = json.loads(json_tag.contents[0].replace("\n", ""))
    return_df = pd.json_normalize(json_obj)
    drop_cols = ['address.@type', 'aggregateRating.@type', 'image']
    for col in drop_cols:
        try:
            return_df.drop(columns=[col], inplace=True, axis=1)
        except KeyError:
            pass
    replacements = [('@', ''), ('address.', ''), ('aggregateRating.', ''), ('.', '_')]
    return_df.columns = [to_snake(string_replacements(elt, replacements)) for elt in return_df.columns]
    return return_df


def hotel_more_meta_data(soup:BeautifulSoup) -> pd.DataFrame:
    js_tags = soup.find_all('script')
    js_data_tag = search_list_soup(js_tags, 'window.utag_data = ')
    json_str = (js_data_tag.contents[0].replace('\n', '').replace('window.utag_data = ', '').split('}')[0]).replace('{', '')
    dict_objs = []
    end_condition = False
    reversed_str = json_str[::-1]
    while not end_condition:
        colon_split = reversed_str.split(':', 1)
        temp_val = colon_split[0]
        comma_split = colon_split[1].split(',', 1)
        temp_key = comma_split[0]
        dict_objs.append('\"' + temp_key[::-1] + '\"' + ' : ' + temp_val[::-1].replace('\'', '\"'))
        if len(comma_split)>1:
            reversed_str = comma_split[1]
        else:
            end_condition = True
    json_obj = json.loads("{ " + " , ".join(dict_objs) + " }")
    return_df = pd.DataFrame([json_obj])
    return_cols = ['hotel_id', 'hotel_class', 'dest_id', 'partner_id', 'channel_id', 'partner_channel_id']
    final_cols = list(set(return_cols).intersection(set(return_df.columns.to_list())))
    return_df = return_df[final_cols]
    missing_cols = list(set(return_cols).difference(set(final_cols)))
    for missing_col in missing_cols:
        return_df[missing_col] = [None]*len(return_df)
    return return_df


def find_sustainable_tag_val(soup:BeautifulSoup) -> str:
    def __helper(soup:BeautifulSoup, tag_type:str='span'):
        sustain_kws = ['Sustainability initiative', 'Travel Sustainable']
        sustain_banner = soup.find('div', attrs={'data-testid':'sustainability-banner-container'})
        if sustain_banner is not None:
            for tag in sustain_banner.find_all(tag_type):
                if tag.string is not None and any(kw in tag.string for kw in sustain_kws):
                    return tag.string
        else:
            return None     
    val = __helper(soup)
    if val is None:
        val = __helper(soup, 'div')
    return val


def get_catagory_reviews(soup:BeautifulSoup) -> dict:
    catreview_dict = {}
    try:
        catreview_tags = soup.select('div[class*=featured_reviews]')[0].findChildren('div', attrs={'data-testid':'review-subscore'})
        for catreview_tag in catreview_tags:
            title = catreview_tag.findChild('span').contents[0]
            rating = None
            for tag in catreview_tag.findChildren('div'):
                if tag.string is not None:
                    try:
                        rating = float(tag.string)
                    except Exception:
                        pass
            catreview_dict[title] = rating
    except Exception:
        pass
    finally:
        return catreview_dict


def get_room_meta_data(soup:BeautifulSoup):
    return_lst = []
    results_lst = soup.find_all('tr')
    for tr_tags in results_lst[1:]:
        try:
            return_dict = {}
            room_type = get_room_type(tr_tags.contents)
            room_dscpt = get_room_dscpt(tr_tags.contents)
            scarcity = get_scarcity(tr_tags.contents)
            if room_type is None:
                room_type = last_room_type
                scarcity = last_scarcity
                room_dscpt = last_room_dscpt
                price_idx = 3
                max_ppl_idx = 1
            else:
                last_room_type = room_type
                last_scarcity = scarcity
                last_room_dscpt = room_dscpt
                price_idx = 5
                max_ppl_idx = 3
            return_dict['room_dscpt'] = room_dscpt
            return_dict['room_type'] = room_type
            return_dict['scarcity'] = scarcity
            return_dict['max_ppl'] = get_max_people(tr_tags.contents, max_ppl_idx)
            return_dict['price'] = get_price(tr_tags.contents, price_idx)
            return_lst.append(return_dict)
        except (AttributeError, UnboundLocalError):
            pass
    return return_lst


def get_room_type(tr_tags:list[element.Tag]):
    try:
        bed_type_tags = tr_tags[1].find('ul', attrs={'class':'rt-bed-types'})
        room_type = ' and '.join([elt.contents[0].replace('\n', '') for elt in bed_type_tags.find_all('span')])
    except Exception:
        room_type = None
    return  room_type



def get_room_dscpt(tr_tags:list[element.Tag]):
    try:
        room_desription = tr_tags[1].find('span', attrs={'class':'hprt-roomtype-icon-link'}).string.replace('\n', '')
    except Exception:
        room_desription = None
    return room_desription


def get_scarcity( tr_tags:list[element.Tag]):
    try:
        return tr_tags[1].find('span', attrs={'class':'only_x_left urgency_message_red'}).string.replace('\n', '')
    except Exception:
        return None


def get_max_people(tr_tags:list[element.Tag], idx:int=3):
    return tr_tags[idx].find('span', attrs={'class':'bui-u-sr-only'}).string.replace('\n', '')


def get_price(tr_tags:list[element.Tag], idx:int=5):
    price_dict = {}
    str_prices = tr_tags[idx].find('span', attrs={'class':'bui-u-sr-only'}).string.split('\n')
    if str_prices[1] == 'Original price':
        price_dict['original_price'] = float(str_prices[2].replace('$','').replace(',',''))
        price_dict['current_price'] = float(str_prices[4].replace('$','').replace(',',''))
    else:
        price_dict['current_price'] = float(str_prices[2].replace('$','').replace(',',''))
    return price_dict
