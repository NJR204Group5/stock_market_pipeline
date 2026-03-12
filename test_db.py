# test_db.py
import psycopg
from psycopg import OperationalError

# PostgreSQL 連線設定，依 Docker 設定修改
DB_CONFIG = {
    "host": "localhost",        # Docker 對外的 port 對應的 host
    "port": 5432,               # Docker PostgreSQL port
    "dbname": "stockdb",        # 資料庫名稱
    "user": "stockuser",         # Docker 建立時的帳號
    "password": "stockpass"      # Docker 建立時設定的密碼
}

def test_db_connection():
    try:
        # psycopg3 直接用 connect 支援 context manager
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print(f"PostgreSQL 連線成功！版本: {version[0]}")
    except OperationalError as e:
        print(f"連線失敗: {e}")
    except Exception as e:
        print(f"發生錯誤: {e}")

if __name__ == "__main__":
    test_db_connection()