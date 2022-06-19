import json
import pandas as pd
import random
import re
import requests
import time
import numpy as np
import logging
from datetime import date

SCHEMA = {
    "condition":"object",
    "created_at":"datetime64[ns]",
    "id":"object",
    "max_quantity":"object",
    "merchant_return_insurance":"bool",
    "min_quantity":"Int64",
    "name":"object",
    "price":"Int64",
    "rush_delivery":"bool",
    "sku_id":"object",
    "state":"object",
    "stock":"Int64",
    "weight":"Int64",
    "category_url":"object",
    "rating_average_rate":"float64",
    "rating_user_count":"Int64",
    "sla_type":"object",
    "sla_value":"object",
    "specs_brand":"object",
    "stats_interest_count":"Int64",
    "stats_sold_count":"Int64",
    "stats_view_count":"Int64",
    "stats_waiting_payment_count":"float64",
    "store_address_city":"object",
    "store_address_province":"object",
    "store_brand_seller":"bool",
    "store_delivery_time":"object",
    "store_id":"object",
    "store_description":"object",
    "store_level_name":"object",
    "store_name":"object",
    "store_premium_level":"object",
    "store_premium_top_seller":"bool",
    "store_rejection_recent_transactions":"Int64",
    "store_rejection_rejected":"Int64",
    "store_reviews_negative":"Int64",
    "store_reviews_positive":"Int64",
    "store_sla_type":"object",
    "store_sla_value":"Int64",
    "store_subscriber_amount":"Int64",
    "store_url":"object",
    "warranty_cheapest":"bool",
    "deal_applied_date":"datetime64[ns]",
    "deal_discount_price":"Int64",
    "deal_expired_date":"datetime64[ns]",
    "deal_original_price":"Int64",
    "deal_percentage":"Int64",
    "url":"object",
    "timestamp":"datetime64[ns]"
}

ADD_COL_TYPE = {
    'datetime': ['timestamp','created_at','deal_applied_date','deal_expired_date'],
    'bool_str': ['merchant_return_insurance', 'rush_delivery', 'store_brand_seller', 'store_premium_top_seller', 'warranty_cheapest'],
}

def get_token():
    res = requests.get("https://bukalapak.com",
                       headers={
                           "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
                       })
    return re.search("\"access_token\":\"(.*?)\"",res.text).group()[16:-1] # looking for token access

def get_scrape (params, get_token, page = 10, URL = "https://api.bukalapak.com/multistrategy-products"):
    DF = []
    index = 1
    while index <= page:
        payload = {
            "offset" : ((index-1)*30),
            "page" : index,
            "access_token" : get_token(),
            **params
        }

        headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
            }
    
        scrape = requests.get (URL, params = payload, headers=headers)
        sleep_time = random.randint(10, 50)
        time.sleep(sleep_time / 1000)

        if scrape.status_code == 200:
            json_file = scrape.json()
            df_1 = pd.json_normalize(json_file)
            df_2 = json.loads(pd.Series.to_json(df_1["data"]))
            df_3 = pd.json_normalize(df_2, record_path="0")
            DF.append(df_3)
        if scrape.status_code != 200:
            raise ValueError("Error returned: {}".format(res.status_code))
        index = index+1
    df_scraper = pd.concat(DF, ignore_index=True)
    return df_scraper


def clean_df (df_scraper, timestr, SCHEMA = SCHEMA, ADD_COL_TYPE = ADD_COL_TYPE):
    df_scraper.columns = [col.replace(' ', '_').replace('.', '_') for col in df_scraper.columns]
    df_scraper['timestamp'] = pd.to_datetime(timestr,format='%Y-%m-%d')

    df_scraper_clean = pd.DataFrame()
    delete = []
    for col in SCHEMA:
        try:
            df_scraper_clean[col] = df_scraper[col]
        except:
            logging.warning("no column %s", col)
            delete.append(col)
    if delete:
        for col in delete:
            SCHEMA = {key:val for key, val in SCHEMA.items() if key != col}

    for col in ADD_COL_TYPE['bool_str']:
        df_scraper_clean[col] = df_scraper_clean[col].astype('bool')

    for col in ADD_COL_TYPE['datetime']:
        df_scraper_clean[col] = pd.to_datetime(df_scraper_clean[col], format="%Y-%m-%dT%H:%M:%SZ")

    df_scraper_clean = df_scraper_clean.astype(SCHEMA).reset_index(drop=True)
    return df_scraper_clean