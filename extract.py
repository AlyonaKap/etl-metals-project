import requests
import json

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By


def get_pgm(metals, start, end):
    data = {
        "_jm_metal_price_portlet_JmMetalPricePortlet_start_Date": start,
        "_jm_metal_price_portlet_JmMetalPricePortlet_end_Date": end,
        "_jm_metal_price_portlet_JmMetalPricePortlet_IntervalType": "DAILY",
    }
    for i, m in enumerate(metals):
        data[f"_jm_metal_price_portlet_JmMetalPricePortlet_selectedMetal{i}"] = m
    return data


def fetch_csv_url(url, data, timeout=15):
    response = requests.post(url=url, data=data, timeout=timeout)
    csv_url = json.loads(response.text)["url"]
    if not csv_url:
        raise ValueError("Missing 'url' in response JSON")
    return csv_url


def to_df_pgm(csv_url):
    df = pd.read_csv(csv_url, header=[1])
    df = df.set_index("Date")
    return df


def requests_extract(url, metals, start, end):
    data = get_pgm(metals, start, end)
    csv_url = fetch_csv_url(url, data)
    df_pgm = to_df_pgm(csv_url)
    return df_pgm


def start_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    return webdriver.Chrome(options=options)


def parse_kitco(driver):
    header = driver.find_element(By.CSS_SELECTOR, "div.grid.grid-cols-5")
    columns = header.text.split("\n")[1:]

    indexes = driver.find_elements(By.TAG_NAME, "time")
    index = [i.text for i in indexes]

    rows = driver.find_elements(
        By.CSS_SELECTOR, "span.mx-auto.block, data.mx-auto.block"
    )
    rows_split = [r.text for r in rows]
    metals = [
        rows_split[i : i + len(index)] for i in range(0, len(rows_split), len(index))
    ]

    today_index = driver.find_element(By.CSS_SELECTOR, "h1.kitco-fix_dateTitle__F8qHQ")
    today_price = driver.find_elements(By.CSS_SELECTOR, "div.kitco-fix_item__InPYR h2")
    price_split = [r.text for r in today_price]

    data_today = pd.DataFrame(
        data=[price_split], index=[today_index.text], columns=columns
    )
    data_history = pd.DataFrame(
        {col: metals[i] for i, col in enumerate(columns)}, index=index
    )
    data = pd.concat([data_today, data_history])
    return data


def parse_yahoo(driver):
    rows = driver.find_elements(By.TAG_NAME, "tr")

    data = []
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) > 0:
            data.append([col.text for col in cols])

    header = driver.find_element(By.TAG_NAME, "thead")
    header_item = header.text.split("\n")
    columns = header_item[0].split(" ") + header_item[1:]

    df = pd.DataFrame(data=data, columns=columns)
    df = df.set_index("Date")
    return df


def selenium_extract(url, res, timeout=30):
    driver = start_driver()
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        df = res(driver)
        return df
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def transform_dfs(df):
    if not df[df.isin(["N/A", "None", "-"]).any(axis=1)].empty:
        df.replace(["N/A", "None", "-"], pd.NA, inplace=True)

    df.index = pd.to_datetime(df.index, errors="coerce", dayfirst=True)
    num_cols = df.columns
    df[num_cols] = df[num_cols].apply(
        lambda col: pd.to_numeric(col.astype(str).str.replace(",", ""), errors="coerce")
    )
    df.sort_index(ascending=False, inplace=True)
    return df
