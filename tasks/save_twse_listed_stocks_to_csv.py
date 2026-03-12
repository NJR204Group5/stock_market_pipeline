import requests
import pandas as pd
import os
import urllib3

from bs4 import BeautifulSoup
from config import HEADERS
from config import OUTPUT_DIR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_twse_listed_stocks():
    print("更新上市股票清單...")
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
    print("完成上市股票清單更新")
    return result_df

def run():
    get_twse_listed_stocks()