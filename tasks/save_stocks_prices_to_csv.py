import random
import certifi
import requests
import pandas as pd
import pandas_market_calendars as mcal
import urllib3
import os
import re
from datetime import datetime
import time

from dateutil.relativedelta import relativedelta
from config import HEADERS
from config import OUTPUT_DIR
from tasks.save_twse_listed_stocks_to_csv import get_twse_listed_stocks

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

tw_calendar = mcal.get_calendar("XTAI") # XTAI = Taiwan Stock Exchange

def verify_and_repair(stock_code, stock_name, df_existing, start_date, file_path, debug=False):
    print(f"{stock_code}{stock_name} 開始完整性檢查...")

    if df_existing is None or df_existing.empty:
        print(f"{stock_code}-{stock_name} 無本地資料，跳過驗證")
        return

    # 統一日期型別
    df_existing["日期"] = pd.to_datetime(df_existing["日期"])

    # 確保 start_date 是 datetime
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)

    if start_date is None:
        print(f"{stock_code} start_date 為 None，跳過")
        return

    now = datetime.now()
    monthly_data = []
    repaired_months = 0

    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= now:
        year = current_date.year
        month = current_date.month

        # 取出本地該月資料
        df_local_month = df_existing[
            (df_existing["日期"].dt.year == current_date.year) &
            (df_existing["日期"].dt.month == current_date.month)
        ]
        # 當月不驗證
        if year == now.year and month == now.month:
            monthly_data.append(df_local_month)
            current_date = current_date + relativedelta(months=1)
            continue

        df_local_month_count = len(df_local_month)
        schedule = tw_calendar.schedule(
            start_date=current_date,
            end_date=current_date + pd.offsets.MonthEnd(0)
        )
        expected_count = len(schedule)

        if expected_count != df_local_month_count:
            # 因為股票有可能停牌或其他原因，所以抓一次 API 用來確認 total
            result = fetch_month_data(stock_code, year, month, debug=debug)
            if isinstance(result, dict) and result.get("type") == "RETRY_WITH_NEW_DATE":
                # API 要求改日期，不需要改
                monthly_data.append(df_local_month)
                # current_date = current_date + relativedelta(months=1)
                continue
            elif isinstance(result, pd.DataFrame):
                # API 回傳 DataFrame，直接使用
                df_api = result.copy()
                if "股票代碼" not in df_api.columns:
                    df_api.insert(0, "股票代碼", stock_code)
                if "股票名稱" not in df_api.columns:
                    df_api.insert(1, "股票名稱", stock_name)

                # 檢查 API 行數是否比本地多
                if len(df_api) > len(df_local_month):
                    print(f"本地資料 {len(df_local_month)} 少於 API {len(df_api)}，補資料 {year}/{month:02d}")
                    monthly_data.append(df_api)
                    repaired_months += 1
                else:
                    monthly_data.append(df_api)
            else:
                # API 無資料，保留原本
                if debug:
                    print(f"{stock_code} {year}-{month:02d} 無可用資料，保留原資料")
                monthly_data.append(df_local_month)
        else:
            monthly_data.append(df_local_month)

        current_date = current_date + relativedelta(months=1)

    # 合併並儲存
    if monthly_data:
        df_final = pd.concat(monthly_data)
        df_final = df_final.drop_duplicates(subset=["日期"], keep="last")
        df_final = df_final.sort_values("日期")
        df_final.to_csv(file_path, index=False)

    print(f"{stock_code}{stock_name} 檢查完成，修復 {repaired_months} 個月份")

def clean_stock_csv(file_path, stock_code=None):
    # 清理重複的股票資料
    if not os.path.exists(file_path):
        print(f"{file_path} 不存在")
        return
    df = pd.read_csv(file_path)

    if "日期" not in df.columns:
        print(f"{file_path} 缺少 '日期' 欄位")
        return

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")

    before = len(df)

    df = df.drop_duplicates(subset=["日期"], keep="last")
    df = df.sort_values("日期")

    after = len(df)

    if stock_code:
        print(f"{stock_code} 清理完成，移除 {before - after} 筆重複資料")
    else:
        print(f"{file_path} 清理完成，移除 {before - after} 筆重複資料")

    df.to_csv(file_path, index=False)

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

    # 在這邊判斷是否已經有stock_code csv了
    file_path = os.path.join(OUTPUT_DIR, f"{stock_code}_full_history.csv")
    if os.path.exists(file_path):
        print(f"{stock_code}{stock_name} 已存在")
        df_existing = pd.read_csv(file_path)
        df_existing["日期"] = pd.to_datetime(df_existing["日期"])
        # 進行完整性驗證與修復
        verify_and_repair(stock_code, stock_name, df_existing, start_date, file_path, debug=debug)

        print(f"{stock_code}{stock_name} 檢查並清理重複資料")
        clean_stock_csv(file_path, stock_code)
        return None
    else:
        current = datetime.now()
        dfs = []
        last_retry = None

        print(f"開始抓取 股票代碼: {stock_code}{stock_name} 全部歷史股價...")
        year, month = start_date.year, start_date.month
        while (year < current.year) or (year == current.year and month <= current.month):
            result = fetch_month_data(stock_code, year, month, debug=debug)
            # API 要求重抓其他日期
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
                dfs.append(result)
                print(f"Current Time: {datetime.now()}, Stock Code: {stock_code}, year/month: {year}/{month:02d} Done!")
            else:
                print(f"Current Time: {datetime.now()}, Stock Code: {stock_code}, year/month: {year}/{month:02d} Failed!")

                # 下一個月
            month += 1
            if month > 12:
                month = 1
                year += 1

            time.sleep(1.5 + random.uniform(0.5, 1.5))

        if not dfs:
            print(f"{stock_code}{stock_name} 從 {year}/{month} 起沒有任何可用資料")
            return None

        final_df = pd.concat(dfs, ignore_index=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        final_df.to_csv(file_path, index=False, encoding="utf-8-sig")

        print(f"Done！共 {len(final_df)} 筆資料")
        print(f"File Path：{file_path}")

        return final_df

def fetch_all_stocks_history(debug=False):
    if os.path.exists(OUTPUT_DIR + "/twse_listed_stocks.csv"):
        twse_listed = pd.read_csv(OUTPUT_DIR + "/twse_listed_stocks.csv")
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
    # 轉成 list of tuples [(stock_code, start_year, start_month), ...]
    stock_list = list(df2[["證券代號", "證券名稱", "上市年", "上市月"]].itertuples(index=False, name=None))
    print("證券代號, 證券名稱和上市年月轉換完成!")
    # print(stock_list)

    for stock_code, stock_name, year, month in stock_list:
        try:
            fetch_full_history(stock_code, stock_name, year, month, debug)
        except Exception as e:
            print(f"{stock_code} 整體失敗: {e}")

def run():
    fetch_all_stocks_history(debug=False)