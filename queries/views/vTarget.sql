CREATE VIEW ihub.vTarget AS 

with ticker_dates AS (
    SELECT ticker, MIN(date) ticker_min_date
    FROM market.price_history
    GROUP BY ticker
)

SELECT bd.ticker, bd.date

    ,CASE 
        WHEN one_wk_avg > ohlc * 5 THEN 1
        WHEN one_wk_avg < ohlc THEN 0
        ELSE (one_wk_avg-ohlc)/(ohlc*(5-1))
    END
    +
    CASE
        WHEN two_wk_avg > ohlc * 3 THEN 1
        WHEN two_wk_avg < ohlc THEN 0
        ELSE (two_wk_avg-ohlc)/(ohlc*(3-1))
    END / 2 AS target

FROM ihub.vBoard_Date bd
LEFT JOIN ticker_dates ON bd.ticker = ticker_dates.ticker

-- Needs to be 60 days past the first offering. Most of these small cap stocks are crazy volitile at the beginning.
WHERE bd.date > ticker_min_date + interval '60 day'

-- Don't set target on tickers with no price
AND bd.ohlc > 0

-- ohlc needs to be greater than .00015 to avoid inactive
AND two_wk_avg > 0.00015

-- Ticker is being actively traded
AND two_wk_vol > 500;