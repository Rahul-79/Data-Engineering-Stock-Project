select 
    symbol,
    current_price,
    round(day_high,2) as day_high,
    round(day_low,2) as day_low,
    round(day_open,2) as day_open,
    round(prev_close,2) as prev_close,
    change_amount,
    round(change_percent,2) as change_percent,
    market_timestamp,
    fetched_at
from {{ ref('bronze_stg_stock_quotes') }}
where current_price is not null
