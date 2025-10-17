import json
import os
from datetime import date, timedelta

from extract import (
    requests_extract,
    selenium_extract,
    parse_kitco,
    parse_yahoo,
    transform_dfs
)
from load import load_to_firebase 


CFG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CFG_PATH, "r", encoding="utf-8") as _f:
    cfg = json.load(_f)

urls = cfg["urls"]
pgm = cfg["pgm"]

today = date.today()
year_ago_date = today - timedelta(days=365)
start_date = year_ago_date.strftime("%d-%m-%Y")
today_date = today.strftime("%d-%m-%Y")

sources = [
    (
        "pgm",
        lambda: requests_extract(urls["pgm"], pgm["metals"], start_date, today_date),
    ),
    ("kitco", lambda: selenium_extract(urls["kitco"], parse_kitco)),
    ("yh_gld", lambda: selenium_extract(urls["yahoo"]["gld"], parse_yahoo)),
    ("yh_sp500", lambda: selenium_extract(urls["yahoo"]["sp500"], parse_yahoo)),
    ("yh_ftse", lambda: selenium_extract(urls["yahoo"]["ftse"], parse_yahoo)),
]

for name, getter in sources:
    try:
        df = getter()
    except Exception as e:
        print(f"Extraction failed for {name}: {e}")
        continue

    if df is None or df.empty:
        continue

    try:
        transform_dfs(df)
        #load_to_csv(df,name)
        load_to_firebase(df, name)
    except Exception as e:
        print(f"Loading failed for {name}: {e}")
