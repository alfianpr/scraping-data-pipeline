import prefect
from bukalapaklib import get_scrape, get_token, clean_df
from datetime import date
from utils import df_to_ps
from prefect.tasks.core.function import FunctionTask
from prefect import Flow, task

get_scrape = FunctionTask(get_scrape)
clean_df = FunctionTask(clean_df)
df_to_ps = FunctionTask(df_to_ps)

@task
def get_today():
    return prefect.context.today

PAGE = 200
CATEGORY_ID = 3263
TABLE_NAME = "buka_kopi"
DATABASE = "boerhand"

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

with Flow(TABLE_NAME) as flow:
    timestr = get_today()
    df_scraper = get_scrape(params, get_token, page=PAGE)
    df_file = clean_df(df_scraper,timestr)
    upload = df_to_ps(df_file, table_name=TABLE_NAME, credential=credential)

#flow.run()
flow.register(project_name="bukalapak")