drop table if exists l1.fact_stock_future_change;

create table if not exists l1.fact_stock_future_change

comment 'l1-未来价格涨跌幅' stored as orc as
select
    trade_date,
    ts_code,
    name,
    close_qfq,
    lead(close_qfq, 3) over(partition by ts_code order by trade_date asc) close_qfq_next3,
    lead(close_qfq, 5) over(partition by ts_code order by trade_date asc) close_qfq_next5,
    lead(close_qfq, 10) over(partition by ts_code order by trade_date asc) close_qfq_next10,
    lead(close_qfq, 20) over(partition by ts_code order by trade_date asc) close_qfq_next20,
    lead(close_qfq, 30) over(partition by ts_code order by trade_date asc) close_qfq_next30,
    min(close_qfq) over(partition by ts_code order by trade_date desc rows between 3 preceding and 1 preceding ) close_qfq_min3,
    min(close_qfq) over(partition by ts_code order by trade_date desc rows between 5 preceding and 1 preceding ) close_qfq_min5,
    min(close_qfq) over(partition by ts_code order by trade_date desc rows between 10 preceding and 1 preceding ) close_qfq_min10,
    min(close_qfq) over(partition by ts_code order by trade_date desc rows between 20 preceding and 1 preceding ) close_qfq_min20,
    min(close_qfq) over(partition by ts_code order by trade_date desc rows between 30 preceding and 1 preceding ) close_qfq_min30,
    max(close_qfq) over(partition by ts_code order by trade_date desc rows between 3 preceding and 1 preceding ) close_qfq_max3,
    max(close_qfq) over(partition by ts_code order by trade_date desc rows between 5 preceding and 1 preceding ) close_qfq_max5,
    max(close_qfq) over(partition by ts_code order by trade_date desc rows between 10 preceding and 1 preceding ) close_qfq_max10,
    max(close_qfq) over(partition by ts_code order by trade_date desc rows between 20 preceding and 1 preceding ) close_qfq_max20,
    max(close_qfq) over(partition by ts_code order by trade_date desc rows between 30 preceding and 1 preceding ) close_qfq_max30,

    low_qfq,
    min(low_qfq) over(partition by ts_code order by trade_date desc rows between 3 preceding and 1 preceding ) low_qfq_min3,
    min(low_qfq) over(partition by ts_code order by trade_date desc rows between 5 preceding and 1 preceding ) low_qfq_min5,
    min(low_qfq) over(partition by ts_code order by trade_date desc rows between 10 preceding and 1 preceding ) low_qfq_min10,
    min(low_qfq) over(partition by ts_code order by trade_date desc rows between 20 preceding and 1 preceding ) low_qfq_min20,
    min(low_qfq) over(partition by ts_code order by trade_date desc rows between 30 preceding and 1 preceding ) low_qfq_min30,
    high_qfq,
    max(high_qfq) over(partition by ts_code order by trade_date desc rows between 3 preceding and 1 preceding ) high_qfq_max3,
    max(high_qfq) over(partition by ts_code order by trade_date desc rows between 5 preceding and 1 preceding ) high_qfq_max5,
    max(high_qfq) over(partition by ts_code order by trade_date desc rows between 10 preceding and 1 preceding ) high_qfq_max10,
    max(high_qfq) over(partition by ts_code order by trade_date desc rows between 20 preceding and 1 preceding ) high_qfq_max20,
    max(high_qfq) over(partition by ts_code order by trade_date desc rows between 30 preceding and 1 preceding ) high_qfq_max30
from l1.daily
where ts_code != 'ts_code'