from bukalapaklib import get_scrape, get_token, clean_df
from datetime import date

timestr = date.today()

params = {
    "prambanan_override" : "true",
    "category_id" : 3263,
    "sort" : "bestselling",
    "limit" : 30,
    "facet" : "true",
    # "brand": "true"
}
df_scraper = get_scrape(params, get_token)
df_file = clean_df(df_scraper,timestr)

df_file.to_csv('kopi_1.csv')