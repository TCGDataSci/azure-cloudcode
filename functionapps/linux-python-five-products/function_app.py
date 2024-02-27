# azure imports 
import azure.functions as func

# tcgds imports
from tcgds.scrapes.five import scrape_all_categories


app = func.FunctionApp()

@app.route("scrapes/five/products")
def five_product_scrape(req:func.HttpRequest):
    scrape_all_categories(progress=False)