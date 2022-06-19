import json
import pandas as pd
import random
import re
import requests
import time
import numpy as np
from bukalapaklib import get_scrape, get_token

params = {
    "prambanan_override" : "true",
    "category_id" : 3263,
    "sort" : "bestselling",
    "limit" : 30,
    "facet" : "true",
    # "brand": "true"
}
df_scraper = get_scrape(params, get_token)
print(df_scraper)