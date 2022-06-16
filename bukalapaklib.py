import json
import pandas as pd
import random
import re
import requests
import time
import numpy as np

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
    return