CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY, -- 自增主鍵
    stock_code VARCHAR(10) NOT NULL, -- 證券代碼
    stock_name VARCHAR(100) NOT NULL, -- 證券名稱
    trade_date DATE NOT NULL, -- 日期
    volume BIGINT, -- 成交股數
    turnover NUMERIC(20, 2), -- 成交金額
    open NUMERIC(10, 2), -- 開盤價
    high NUMERIC(10, 2), -- 最高價
    low NUMERIC(10, 2), -- 最低價
    close NUMERIC(10, 2), -- 收盤價
    change NUMERIC(10, 2), -- 漲跌價差
    transactions BIGINT, -- 成交筆數
    note TEXT, -- 註記

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 建立時間
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- 更新時間
);
-- 建立 複合唯一鍵，同一個股票+同一天的交易資料只能有一筆
ALTER TABLE stock_prices
ADD CONSTRAINT uq_stock_date UNIQUE (stock_code, trade_date);

-- 常用索引
CREATE INDEX idx_stock_code ON stock_prices(stock_code);
CREATE INDEX idx_trade_date ON stock_prices(trade_date);

-- 建立 trigger 更新 updated_at
CREATE TRIGGER update_stock_prices_updated_at
BEFORE UPDATE ON stock_prices
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();