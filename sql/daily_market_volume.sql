WITH 
    ideamarket_lifetime_view AS (SELECT * FROM generate_series('2021-02-15', current_date, '1 day') AS gen_date),
    trade_gen_table AS 
    (
    SELECT * FROM ideamarket_lifetime_view
    CROSS JOIN UNNEST ('{"Buy","Sell"}'::text[]) gen_user_action
    CROSS JOIN (SELECT DISTINCT("marketID") AS gen_market_id FROM ideamarket."IdeaTokenExchange_evt_InvestedState") gen_market_id
    ),
    token_overview AS
    (
    SELECT 
        'Buy' AS user_action,
        call_tx_hash
    FROM ideamarket."IdeaTokenExchange_call_buyTokens"
    WHERE call_success = 'true'
    UNION ALL 
    SELECT 
        'Sell' AS user_action,
        call_tx_hash
    FROM ideamarket."IdeaTokenExchange_call_sellTokens"
    WHERE call_success = 'true'
    ),
    trade_totals_grouped AS 
    (
    SELECT 
        date_trunc('day', evt_block_time) AS trade_date,
        user_action,
        "marketID" as market_id,
        SUM(volume / 10 ^ 18) AS total_amount
    FROM ideamarket."IdeaTokenExchange_evt_InvestedState"
    JOIN token_overview ON call_tx_hash = evt_tx_hash
    GROUP BY 1,2,3
    ),
    daily_trade_totals_lifetime AS 
    (
    SELECT 
        gen_date,
        gen_user_action,
        market_id,
        COALESCE(total_amount, 0) AS volume
    FROM trade_gen_table
    LEFT JOIN trade_totals_grouped ON 
        gen_date = trade_date AND gen_user_action = user_action AND gen_market_id = market_id
    )

SELECT * FROM daily_trade_totals_lifetime