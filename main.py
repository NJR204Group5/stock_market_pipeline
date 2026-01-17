import requests
import pandas as pd
import io
from datetime import datetime, timedelta
import time
import os

def get_twse_listed_stocks():
# TWSE 上市清單（集中市場）
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    headers = { "User-Agent": "Mozilla/5.0" }
    res = requests.get(url, headers=headers, verify=False)
    res.encoding = "big5"  # 官方頁面是 big5 編碼

    # 用 pandas 讀出 HTML 裡的 table
    df = pd.read_html(io.StringIO(res.text))[0]

    # 第一列往往是表頭，整理成欄位名稱
    # print(df.iloc[1])
    df.columns = df.iloc[0]
    df = df.iloc[2:].reset_index(drop=True)

    # 有些欄位可能空白或亂碼，重新整理基本欄位
    df = df.rename(columns={
        "有價證券代號及名稱": "證券代號及名稱",
        "國際證券辨識號碼(ISIN Code)": "ISIN Code",
        "上市日": "上市日",
        "市場別": "市場別",
        "產業別": "產業別"
    })

    # 全形空格換成半形空格
    df["證券代號及名稱"] = df["證券代號及名稱"].str.replace("\u3000", " ") #空格是全形 \u3000

    # 把 代號與名稱分成兩欄
    df[["證券代號", "證券名稱"]] = df["證券代號及名稱"].str.split(" ", n = 1, expand = True)

    # print(df[["證券代號", "證券名稱", "ISIN Code", "上市日", "市場別", "產業別"]].head(10))
    return df[["證券代號", "證券名稱", "ISIN Code", "上市日", "市場別", "產業別"]]

twse_listed = get_twse_listed_stocks()
print(twse_listed.head(30))
print("總上市股票數量:", len(twse_listed))