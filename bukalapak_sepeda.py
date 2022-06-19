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
    "category_id" : 2308,
    "sort" : "bestselling",
    "limit" : 30,
    "facet" : "true",
    # "brand": "true"
}
get_scrape(params, get_token)