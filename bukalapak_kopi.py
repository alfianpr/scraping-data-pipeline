from bukalapaklib import get_scrape, get_token, clean_df
from datetime import date
from utils import df_to_ps

timestr = date.today()

PAGE = 5
CATEGORY_ID = 3263
TABLE_NAME = "buka_kopi"
DATABASE = "Sandbox"

params = {
    "prambanan_override" : "true",
    "category_id"        : CATEGORY_ID,
    "sort"               : "bestselling",
    "limit"              : 30,
    "facet"              : "true",
    #"brand"             : "true"
}

credential = {
    'username' : 'boerhand',
    'password' : '1232',
    'host'     : '34.128.95.217',
    'port'     : '5432',
    'db_name'  : DATABASE
}

df_scraper = get_scrape(params, get_token, page=PAGE)
df_file = clean_df(df_scraper,timestr)
df_to_ps(df_file, table_name=TABLE_NAME, credential=credential)