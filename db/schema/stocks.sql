CREATE TABLE stocks (
    id SERIAL PRIMARY KEY, -- 自增主鍵
    stock_code VARCHAR(10) UNIQUE NOT NULL, -- 證券代號
    stock_name VARCHAR(100) NOT NULL, -- 證券名稱
    category VARCHAR(100), -- 證券類別
    isin_code VARCHAR(20), -- ISIN Code
    listed_date DATE NOT NULL, -- 上市日
    industry VARCHAR(100), -- 產業別
    delisted_date DATE, -- 下市日
    market VARCHAR(20), -- 上市市場(ex: TWSE, OTC...)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 建立時間
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- 更新時間
);
CREATE INDEX idx_industry ON stocks(industry);
CREATE INDEX idx_listed_date ON stocks(listed_date);

-- 建立 trigger 更新 updated_at
CREATE TRIGGER update_stocks_updated_at
BEFORE UPDATE ON stocks
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();