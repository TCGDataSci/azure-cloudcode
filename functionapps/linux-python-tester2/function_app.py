# others
import time
import requests
import pandas as pd
import requests.adapters
from contextlib import ExitStack

# azure imports 
import azure.functions as func

# tcgds imports
from tcgds.jobs import InstanceMetric
from tcgds.scrape import ResponseCodes
from tcgds.postgres import PandasPGHelper
from tcgds.reporting import EmailExceptionHandler
from tcgds.scrape import Scraper, Step, base_headers
from tcgds.scrapes.bookstoscrape import catalogue_page_parser, book_page_parser, get_next_catalogue_page, init_step, base_url, catalogue_url, append_url



app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route('testing/bookstoscrape')
def bookstocrape_scrape(req:func.HttpRequest):
    with ExitStack() as stack:
        req_obj:dict = req.get_json()

        # initialize exception handler
        exc_handler = stack.enter_context(EmailExceptionHandler())
        exc_handler.subject = 'Bookstoscrape Exception'
        
        # parse request object
        job_name = req_obj['job_name']
        instance_id = req_obj['instance_id']
        req_body = req_obj.get('body')
        if req_body is not None:
            pass

        # enter scraper context
        scraper = stack.enter_context(Scraper(job_name=job_name, instance_id=instance_id, auth_environment='local'))
        
        # initialize scraper
        print(scraper.route)
        if not scraper.initialized:
            scraper.init_route(steps=init_step)
            scraper.checkpoint()
            metrics = [
                InstanceMetric(metric_name='total_requests', value=0, instance_id=instance_id),
                InstanceMetric(metric_name='failed_requests', value=0, instance_id=instance_id),
                InstanceMetric(metric_name='successful_requests', value=0, instance_id=instance_id)
            ]
            scraper.add_metrics(metrics)


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

        # add parsers (must be in order of route level)
        scraper.route.add_parsers([catalogue_page_parser, book_page_parser])


        # scrape
        catalogue_df = pd.DataFrame()
        book_data_lst = list()
        for url, parser in scraper.route:
            
            # make request
            response = req_session.get(url)
            
            # update request metrics
            scraper.metrics['total_requests'].value+=1
            if response.status_code!=ResponseCodes('successful'):
                scraper.metrics['failed_requests'].value+=1
            else:
                scraper.metrics['successful_requests'].value+=1
            scraper.update_metrics()

            # parse response
            parsed_data = parser(response.text)
            
            # level 0 logic
            if scraper.route.level==0:

                # merge data after scraping all books from single catalogue page and commit to postgres
                if scraper.route.changed_level_up:
                    book_df = pd.DataFrame(book_data_lst)
                    merged_df = catalogue_df.merge(book_df, on='title', how='left')
                    affected_rows = PandasPGHelper(auth_environment='local').to_sql(merged_df, 'book_data', 'bookstoscrape', affected_rows=True)
                    # reset data objects
                    catalogue_df = pd.DataFrame()
                    book_data_lst.clear()
                    # checkpoint
                    scraper.checkpoint()

                # update data holder
                temp_df = pd.DataFrame(parsed_data)
                catalogue_df = pd.concat([temp_df, catalogue_df], ignore_index=True)

                # add book pages to next steps from parsed data
                next_steps = [Step(base_url+append_url.format(href=href), 1) for href in temp_df['href']]
                scraper.route.add(next_steps)

                # add next catalogue page if it exists
                next_catalogue_page = get_next_catalogue_page(response.text)
                if next_catalogue_page is not None:
                    next_catalogue_step = Step(base_url+append_url.format(href=next_catalogue_page), 0) 
                    scraper.route.add(next_catalogue_step)

            # level 1 logic
            elif scraper.route.level==1:
                # create book_page_data in data dict
                book_data_lst.append(parsed_data)                


            time.sleep(throttle)