
import random
import certifi
import psycopg
import requests
import pandas as pd
import pandas_market_calendars as mcal
import urllib3
import re
from datetime import datetime
import time

from config import HEADERS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

tw_calendar = mcal.get_calendar("XTAI") # XTAI = Taiwan Stock Exchange

# PostgreSQL 連線設定
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "stockdb",
    "user": "stockuser",
    "password": "stockpass"
}

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
    roc_year, month, day = map(int, m.groups())
    year = roc_year + 1911

    return f"{year:04d}-{month:02d}-{day:02d}"

def fetch_month_data(stock_code, year, month, retry=5, debug=False):
    # 抓取某股票某年月資料，成功回傳 DataFrame，否則回傳 None
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?response=json&date={year}{month:02d}01&stockNo={stock_code}"
    )

    for attempt in range(1, retry + 1):
        try:
            time.sleep(0.5)

            res = requests.get(url, headers=HEADERS, timeout=(10, 40), verify=certifi.where())
            res.raise_for_status()
            data = res.json()
            if debug:
                print(f"[DEBUG] {stock_code} {year}/{month:02d} → {data}")
        except requests.exceptions.ReadTimeout:
            print(f"[Timeout] {stock_code} {year}/{month:02d} 第 {attempt}/{retry}")
        except requests.exceptions.ConnectTimeout:
            print(f"[ConnectTimeout] {stock_code} {year}/{month:02d} 第 {attempt}/{retry}")
            # time.sleep(3 + random.random() * 2)
            # continue
        except requests.exceptions.HTTPError as e:
            print(f"[HTTPError] {e}")
            break  # 4xx 通常沒必要重試
        except requests.exceptions.RequestException as e:
            print(f"[RequestError] {e}")
        except ValueError:
            print(f"[JSON解析失敗] {stock_code} {year}/{month:02d}")
        else:
            # 成功拿到 data
            stat = data.get("stat", "")
            total = data.get("total", 0)
            rows = data.get("data")

            if stat == "OK" and rows:
                # 建立 DataFrame
                df = pd.DataFrame(data["data"], columns=[c.strip() for c in data["fields"]])

                # 民國年轉西元
                try:
                    date_parts = df["日期"].astype(str).str.strip().str.split("/", expand=True)
                    date_df = pd.DataFrame({
                        "year": date_parts[0].astype(int) + 1911,
                        "month": date_parts[1].astype(int),
                        "day": date_parts[2].astype(int),
                    })
                    df["日期"] = pd.to_datetime(date_df)
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
                    # 確保回傳 datetime
                    if isinstance(retry_date, str):
                        retry_date = pd.to_datetime(retry_date)
                    return {
                        "type": "RETRY_WITH_NEW_DATE",
                        "date": retry_date
                    }

            # 沒資料
            if "沒有符合條件" in stat:
                return None

            # 其他情況重試
            print(f"[異常狀態] {stat}")

        # 進入重試
        sleep_time = min(2 ** attempt, 30) + random.random() # 指數避退
        time.sleep(sleep_time)

    print(f"[失敗] {stock_code} {year}/{month:02d} 超過最大重試")
    return None

def get_valid_start_year_month(stock_code, start_year, start_month):
    # 嘗試抓指定年月的資料，如果TWSE要求改查更早日期，就回傳datetime物件
    result = fetch_month_data(stock_code, start_year, start_month)
    if isinstance(result, dict) and result.get("type") == "RETRY_WITH_NEW_DATE":
        retry_date = result.get("date")
        return retry_date
    return None

def fetch_full_history(stock_code, stock_name, start_year, start_month, debug=False):
    if start_year is None:
        print(f"找不到 {stock_code}{stock_name} 的任何歷史資料")
        return None

    start_date = get_valid_start_year_month(
        stock_code,
        start_year,
        start_month
    )
    print(f"API 要求改查 {start_date}")
    if start_date is None:
        print(f"{stock_code} 找不到有效起始日期")
        return None

    current = datetime.now()
    last_retry = None

    print(f"開始抓取 股票代碼: {stock_code}{stock_name} 全部歷史股價...")
    year, month = start_date.year, start_date.month

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            while (year < current.year) or (year == current.year and month <= current.month):
                month_start = datetime(year, month, 1)
                month_end = month_start + pd.offsets.MonthEnd(0)

                cur.execute("""
                    SELECT COUNT(*) FROM stock_prices
                    WHERE stock_code = %s
                    AND trade_date BETWEEN %s AND %s
                """, (stock_code, month_start, month_end))

                count = cur.fetchone()[0]

                if count > 0:
                    if debug:
                        print(f"{stock_code} {year}/{month:02d} DB 已有 {count} 筆資料，跳過")

                    month += 1
                    if month > 12:
                        month = 1
                        year += 1
                    continue

                result = fetch_month_data(stock_code, year, month, debug=debug)
                # API 要求改查其他日期
                if isinstance(result, dict) and result.get("type") == "RETRY_WITH_NEW_DATE":
                    retry_dt = result["date"]
                    if retry_dt is None:
                        print(f"{stock_code} API 返回日期為 None，跳過")
                        month += 1
                        continue
                    year, month = retry_dt.year, retry_dt.month

                    if last_retry == (year, month):
                        print(f"API 重複要求 {year}/{month:02d}，跳過避免死循環")
                        month += 1
                        continue
                    last_retry = (year, month)
                    year, month = year, month
                    print(f"API 要求改查 {year}/{month:02d}")
                    continue

                last_retry = None

                if isinstance(result, pd.DataFrame):
                    # 加上股票代碼與名稱欄位
                    result.insert(0, "股票代碼", stock_code)
                    result.insert(1, "股票名稱", stock_name)
                    result = result.replace("--", None)
                    result = result.replace(",", "", regex=True)
                    result["漲跌價差"] = result["漲跌價差"].str.replace("X", "", regex=False)
                    result["日期"] = pd.to_datetime(result["日期"])
                    sql = """
                        INSERT INTO stock_prices
                        (
                            stock_code,
                            stock_name,
                            trade_date,
                            volume,
                            turnover,
                            open,
                            high,
                            low,
                            close,
                            change,
                            transactions,
                            note
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stock_code, trade_date)
                        DO UPDATE SET
                            stock_name = excluded.stock_name,
                            volume = excluded.volume,
                            turnover = excluded.turnover,
                            open = excluded.open,
                            high = excluded.high,
                            low = excluded.low,
                            close = excluded.close,
                            change = excluded.change,
                            transactions = excluded.transactions,
                            note = excluded.note
                        """
                    data = [
                        (
                            row["股票代碼"],
                            row["股票名稱"],
                            row["日期"],
                            row["成交股數"],
                            row["成交金額"],
                            row["開盤價"],
                            row["最高價"],
                            row["最低價"],
                            row["收盤價"],
                            row["漲跌價差"],
                            row["成交筆數"],
                            row["註記"]
                        )
                        for _, row in result.iterrows()
                    ]

                    cur.executemany(sql, data)
                    conn.commit()
                    print(f"Current Time: {datetime.now()}, Stock: {stock_code}{stock_name}, year/month: {year}/{month:02d} Done!")
                else:
                    print(f"Current Time: {datetime.now()}, Stock: {stock_code}{stock_name}, year/month: {year}/{month:02d} Failed!")

                    # 下一個月
                month += 1
                if month > 12:
                    month = 1
                    year += 1

                time.sleep(1.5 + random.uniform(0.5, 1.5))
    print(f"{stock_code}{stock_name} 全部歷史資料寫入完成")

def fetch_all_stocks_history(debug=False):
    # 從 DB 讀取上市股票清單
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("""
                SELECT stock_code, stock_name, listed_date
                FROM stocks
            """)
            rows = cur.fetchall()

    df = pd.DataFrame(rows)
    if df.empty:
        print("stocks table 沒有資料")
        return

    # 確保 listed_date 是 datetime
    df["listed_date"] = pd.to_datetime(df["listed_date"], errors="coerce")

    # 取年份與月份
    df["上市年"] = df["listed_date"].dt.year
    df["上市月"] = df["listed_date"].dt.month

    stock_list = list(df[["stock_code", "stock_name", "上市年", "上市月"]].itertuples(index=False, name=None))
    print(f"證券代號, 證券名稱和上市年月轉換完成! 共 {len(stock_list)} 檔股票")

    for stock_code, stock_name, year, month in stock_list:
        try:
            fetch_full_history(stock_code, stock_name, year, month, debug)
        except Exception as e:
            print(f"{stock_code} 整體失敗: {e}")

def run():
    fetch_all_stocks_history(debug=False)