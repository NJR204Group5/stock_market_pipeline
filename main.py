import sys
import argparse
from tasks.save_twse_listed_stocks_to_csv import run as run_stocks_to_csv
from tasks.save_stocks_prices_to_csv import run as run_stock_prices_to_csv
from tasks.save_twse_listed_stocks_to_db import run as run_stocks_to_db
from tasks.save_stocks_prices_to_db import run as run_stock_prices_to_db

TASKS = {
    "stocks_to_csv": run_stocks_to_csv,
    "stock_prices_to_csv": run_stock_prices_to_csv,
    "stocks_to_db": run_stocks_to_db,
    "stock_prices_to_db": run_stock_prices_to_db,
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "task",
        nargs="?",
        default="stocks_to_db",
        choices=TASKS.keys(),
        help="選擇要執行的任務"
    )
    args = parser.parse_args()

    try:
        TASKS[args.task]()
        print("任務執行完成")
    except Exception as e:
        print(f"任務執行失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()