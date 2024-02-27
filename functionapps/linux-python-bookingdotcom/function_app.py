# azure imports 
import azure.functions as func

# other imports
import uuid

# tcgds imports
from tcgds.reporting import aioreport
from tcgds.scrapes.bookingdotcom import USHotelScrape
from tcgds.scrape import base_headers


app = func.FunctionApp()

@app.route("scrape/bookingdotcom/ushotels")
async def bookingdotcom_ushotel_scrape(req:func.HttpRequest):
    instanceId = uuid.uuid4().hex
    throttle = 8
    retries = 1 
    async with USHotelScrape(instance_id=instanceId) as scraper: 
        scraper.set_request_kwargs(headers=base_headers, retries=retries, wait_time=throttle)
        report_scrape = aioreport('Production '+scraper.scrape_name)(scraper.scrape)
        await report_scrape(throttle=throttle)
    return 'success'

