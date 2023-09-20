drop table if exists l1.fact_stock_tag_price_prev;

create table if not exists l1.fact_stock_tag_price_prev

comment 'l1标签-近期价格标签' stored as orc as

SELECT
    ts_code,
    trade_date,
    close_qfq,
    is_min_close_qfq3,
    is_min_close_qfq5,
    is_min_close_qfq10,
    is_min_close_qfq20,
    is_min_close_qfq60,

    is_max_close_qfq3,
    is_max_close_qfq5,
    is_max_close_qfq10,
    is_max_close_qfq20,
    is_max_close_qfq60,
    low_qfq,
    is_min_low_qfq3,
    is_min_low_qfq5,
    is_min_low_qfq10,
    is_min_low_qfq20,
    is_min_low_qfq60,
    high_qfq,
    is_max_high_qfq3,
    is_max_high_qfq5,
    is_max_high_qfq10,
    is_max_high_qfq20,
    is_max_high_qfq60,

    if(is_min_low_qfq3 = is_min_low_qfq60, 1, 0) as is_pre3_min_low60,
    if(is_min_low_qfq5 = is_min_low_qfq60, 1, 0) as is_pre5_min_low60,
    if(is_min_low_qfq10 = is_min_low_qfq60, 1, 0) as is_pre10_min_low60,
    if(is_max_high_qfq3 = is_max_high_qfq60, 1, 0) as is_pre3_max_high60,
    if(is_max_high_qfq5 = is_max_high_qfq60, 1, 0) as is_pre5_max_high60,
    if(is_max_high_qfq10 = is_max_high_qfq60, 1, 0) as is_pre10_max_high60
from (
    SELECT
        trade_date,
        ts_code,
        close_qfq,
        if(close_qfq = min_close3, 1, 0) as is_min_close_qfq3,
        if(close_qfq = min_close5, 1, 0) as is_min_close_qfq5,
        if(close_qfq = min_close10, 1, 0) as is_min_close_qfq10,
        if(close_qfq = min_close20, 1, 0) as is_min_close_qfq20,
        if(close_qfq = min_close60, 1, 0) as is_min_close_qfq60,

        if(close_qfq = max_close3, 1, 0) as is_max_close_qfq3,
        if(close_qfq = max_close5, 1, 0) as is_max_close_qfq5,
        if(close_qfq = max_close10, 1, 0) as is_max_close_qfq10,
        if(close_qfq = max_close20, 1, 0) as is_max_close_qfq20,
        if(close_qfq = max_close60, 1, 0) as is_max_close_qfq60,
        low_qfq,
        if(low_qfq = min_low3, 1, 0) as is_min_low_qfq3,
        if(low_qfq = min_low5, 1, 0) as is_min_low_qfq5,
        if(low_qfq = min_low10, 1, 0) as is_min_low_qfq10,
        if(low_qfq = min_low20, 1, 0) as is_min_low_qfq20,
        if(low_qfq = min_low60, 1, 0) as is_min_low_qfq60,
        high_qfq,
        if(high_qfq = max_high3, 1, 0) as is_max_high_qfq3,
        if(high_qfq = max_high5, 1, 0) as is_max_high_qfq5,
        if(high_qfq = max_high10, 1, 0) as is_max_high_qfq10,
        if(high_qfq = max_high20, 1, 0) as is_max_high_qfq20,
        if(high_qfq = max_high60, 1, 0) as is_max_high_qfq60

    from (
        SELECT
            ts_code,
            trade_date,
            close_qfq,
            min(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 2 preceding AND CURRENT ROW) min_close3,
            min(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 4 preceding AND CURRENT ROW) min_close5,
            min(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 9 preceding AND CURRENT ROW) min_close10,
            min(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 19 preceding AND CURRENT ROW) min_close20,
            min(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 59 preceding AND CURRENT ROW) min_close60,
            max(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 2 preceding AND CURRENT ROW) max_close3,
            max(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 4 preceding AND CURRENT ROW) max_close5,
            max(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 9 preceding AND CURRENT ROW) max_close10,
            max(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 19 preceding AND CURRENT ROW) max_close20,
            max(close_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 59 preceding AND CURRENT ROW) max_close60,
            low_qfq,
            min(low_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 2 preceding AND CURRENT ROW) min_low3,
            min(low_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 4 preceding AND CURRENT ROW) min_low5,
            min(low_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 9 preceding AND CURRENT ROW) min_low10,
            min(low_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 19 preceding AND CURRENT ROW) min_low20,
            min(low_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 59 preceding AND CURRENT ROW) min_low60,
            high_qfq,
            max(high_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 2 preceding AND CURRENT ROW) max_high3,
            max(high_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 4 preceding AND CURRENT ROW) max_high5,
            max(high_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 9 preceding AND CURRENT ROW) max_high10,
            max(high_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 19 preceding AND CURRENT ROW) max_high20,
            max(high_qfq) over(PARTITION BY ts_code ORDER BY trade_date ASC ROWS BETWEEN 59 preceding AND CURRENT ROW) max_high60
        FROM l1.daily
    )  t
) tt

