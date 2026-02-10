import random
from unittest import result

import certifi
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import io
import urllib3
from datetime import datetime, timedelta
import time
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.twse.com.tw/zh/trading/historical/stock-day.html",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}
OUTPUT_DIR = "twse_stock_history"

def get_twse_listed_stocks():
    # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    res = requests.get(url, headers=HEADERS, verify=False, timeout=30)
    res.encoding = "big5"  # 官方頁面是 big5 編碼

    soup = BeautifulSoup(res.text, "html.parser")

    # 取得第一個 table（TWSE 上市清單）
    table = soup.find("table", {"class": "h4"})
    rows = table.find_all("tr")

    data = []
    current_category = None

    for row in rows:
        cols = [col.get_text(strip=True) for col in row.find_all("td")]
        if not cols:
            continue
        # 類別列（不是股票代碼開頭）
        if not cols[0][:1].isdigit():
            current_category = cols[0]
            continue
        data.append([
            cols[0],
            cols[1],
            current_category,
            cols[2],
            cols[3],
            cols[4] if len(cols) > 4 else None
        ])
    df = pd.DataFrame(
        data,
        columns=[
            "證券代號及名稱",
            "ISIN Code",
            "證券類別",
            "上市日",
            "市場別",
            "產業別",
        ],
    )
    # 全形空格換成半形空格
    df["證券代號及名稱"] = df["證券代號及名稱"].str.replace("\u3000", " ")  # 空格是全形 \u3000

    # 把 代號與名稱分成兩欄
    df[["證券代號", "證券名稱"]] = df["證券代號及名稱"].str.split(" ", n=1, expand=True)

    result_df = df[["證券代號", "證券名稱", "證券類別", "ISIN Code", "上市日", "市場別", "產業別"]]
    # print(result_df.head(10))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, "twse_listed_stocks.csv")
    result_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    return result_df

def parse_retry_date_from_stat(stat: str):
    # 從 TWSE stat 字串中解析「請重新查詢」的日期，回傳 (year, month)（西元），解析不到回傳 None
    if not stat:
        return None
    if "重新查詢" not in stat:
        return None

    # 抓民國年月日
    m = re.search(r'(\d+)年(\d+)月(\d+)日', stat)
    if not m:
        return None
    roc_year, month, _ = map(int, m.groups())
    year = roc_year + 1911

    return year, month

def fetch_full_history(stock_code, stock_name, start_year, start_month, debug=False):
    if start_year is None:
        print(f"找不到 {stock_code} 的任何歷史資料")
        return None

    current = datetime.now()
    dfs = []
    last_retry = None

    print(f"開始抓取 股票代碼: {stock_code} 全部歷史股價...")
    year, month = start_year, start_month
    while (year < current.year) or (year == current.year and month <= current.month):
        result = fetch_month_data(stock_code, year, month, debug=debug)

        if isinstance(result, dict) and result.get("type") == "RETRY_WITH_NEW_DATE":
            new_year = result.get("year")
            new_month = result.get("month")

            if last_retry == (new_year, new_month):
                print(f"API 重複要求 {new_year}/{new_month:02d}，跳過避免死循環")
                month += 1
                continue

            last_retry = (new_year, new_month)
            year, month = new_year, new_month
            print(f"API 要求改查 {year}/{month:02d}")
            continue

        last_retry = None

        if isinstance(result, pd.DataFrame):
            result.insert(0, "股票代碼", stock_code)
            result.insert(1, "股票名稱", stock_name)
            dfs.append(result)
            # print(result)
            print(f"Current Time: {datetime.now()}, Stock Code: {stock_code}, year/month: {year}/{month:02d} Done!")
        else:
            print(f"Current Time: {datetime.now()}, Stock Code: {stock_code}, year/month: {year}/{month:02d} Failed!")

        # 下一個月
        month = month + 1
        if month > 12:
            month = 1
            year = year + 1

        time.sleep(1.5 + random.uniform(0.5, 1.5))

    if not dfs:
        print(f"{stock_code} 從 {start_year}/{start_month} 起沒有任何可用資料")
        return None

    final_df = pd.concat(dfs, ignore_index=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, f"{stock_code}_full_history.csv")
    final_df.to_csv(file_path, index=False, encoding="utf-8-sig")

    print(f"Done！共 {len(final_df)} 筆資料")
    print(f"File Path：{file_path}")

    return final_df

def fetch_month_data(stock_code, year, month, retry=2, debug=False):
    # 抓取某股票某年月資料，成功回傳 DataFrame，否則回傳 None
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?response=json&date={year}{month:02d}01&stockNo={stock_code}"
    )

    for attempt in range(retry + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=40, verify=certifi.where())
            data = res.json()
            if debug:
                print(f"[DEBUG] {stock_code} {year}/{month:02d} → {data}")
        except requests.exceptions.ConnectTimeout:
            print(f"connection timeout {stock_code} {year}/{month:02d}，retry {attempt + 1}/3")
            time.sleep(3 + random.random() * 2)
            continue
        except Exception as e:
            print(f"Request error {stock_code} {year}/{month:02d}: {e}")
            time.sleep(3 + attempt * 2)
            continue

        stat = data.get("stat", "")
        total = data.get("total", 0)
        rows = data.get("data")

        if stat == "OK" and rows:
            # 建立 DataFrame
            df = pd.DataFrame(data["data"], columns=[c.strip() for c in data["fields"]])

            # 日期轉換
            try:
                df["日期"] = pd.to_datetime(
                    df["日期"].apply(
                        lambda x: f"{int(x.strip().split('/')[0]) + 1911}-"
                                  f"{x.strip().split('/')[1].zfill(2)}-"
                                  f"{x.strip().split('/')[2].zfill(2)}"
                    )
                )
            except Exception as e:
                print(f"日期轉換錯誤 {stock_code} {year}/{month:02d}: {e}")
                return None

            # 數值欄位
            num_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")

            return df

        # API 不支援的時間（不用看中文字）
        if total == 0 and ("重新查詢" in stat or "查詢日期小於" in stat):
            retry_date = parse_retry_date_from_stat(stat)
            if retry_date:
                retry_year, retry_month = retry_date
                return {
                    "type": "RETRY_WITH_NEW_DATE",
                    "year": retry_year,
                    "month": retry_month
                }

        # 沒資料或被擋
        if stat != "OK" or not data.get("data"):
            if "沒有符合條件" in stat and year < datetime.now().year:
                print(f"疑似被擋 {stock_code} {year}/{month:02d}，重試 {attempt + 1}/{retry}")
                time.sleep(5 + attempt * 3)
                continue
            return None

    return None

def fetch_all_stocks_history(debug=False):
    if os.path.exists("twse_stock_history/twse_listed_stocks.csv"):
        twse_listed = pd.read_csv("twse_stock_history/twse_listed_stocks.csv")
    else:
        twse_listed = get_twse_listed_stocks()
    # print(twse_listed.head(30))
    df2 = twse_listed[["證券代號", "證券名稱", "上市日"]].copy()
    # 轉成 datetime
    df2["上市日"] = pd.to_datetime(df2["上市日"], format="%Y/%m/%d")
    # 取年份與月份
    df2["上市年"] = df2["上市日"].dt.year
    df2["上市月"] = df2["上市日"].dt.month
    # print("總上市股票數量:", len(twse_listed))
    # 轉成 list of tuples [(stock_no, start_year, start_month), ...]
    stock_list = list(df2[["證券代號", "證券名稱", "上市年", "上市月"]].itertuples(index=False, name=None))
    print("證券代號, 證券名稱和上市年月轉換完成!")
    # print(stock_list)

    for stock_no, stock_name, year, month in stock_list:
        # 在這邊判斷是否已經有stock_no csv了
        file_path = os.path.join(OUTPUT_DIR, f"{stock_no}_full_history.csv")
        if os.path.exists(file_path):
            print(f"{stock_no} 已存在，跳過抓取")
            continue
        try:
            fetch_full_history(stock_no, stock_name, year, month, debug)
        except Exception as e:
            print(f"{stock_no} 整體失敗: {e}")

fetch_all_stocks_history(False)
# fetch_full_history('1101', "台泥", 1910, 1, True)
# get_twse_listed_stocks()