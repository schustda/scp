CREATE VIEW ihub.vBoard_Date AS 

    with ih AS (
        SELECT date, ticker, sentiment_polarity, sentiment_subjectivity, posts, RANK() OVER(PARTITION BY date ORDER BY posts DESC) daily_ranking
        FROM (
            SELECT CAST(ms.message_date AS date) date, ticker, AVG(sentiment_polarity) sentiment_polarity, AVG(sentiment_subjectivity) sentiment_subjectivity, COUNT(*) posts
            FROM ihub.message_sentiment ms
            LEFT JOIN items.symbol s ON ms.ihub_code = s.ihub_code
            WHERE exchange = 'usotc'
            GROUP BY CAST(ms.message_date AS date), ticker
        ) x
    )

    SELECT ph.date,ph.ticker,sentiment_polarity,sentiment_subjectivity,posts,daily_ranking,ohlc,dollar_volume,one_wk_avg,two_wk_avg,two_wk_vol,target
    FROM market.price_history ph
    LEFT JOIN ih
    ON ph.ticker = ih.ticker
    AND ph.date = ih.date;