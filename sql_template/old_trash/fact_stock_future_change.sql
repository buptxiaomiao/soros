drop table if exists l1.fact_stock_future_change;

create table if not exists l1.fact_stock_future_change(

    ts_code             string  comment 'TS代码',
    name                string  comment '股票名称',
    trade_date          string  comment '交易日期',
    close_qfq           float   comment '收盘价-TS前复权',
    close_qfq_next1     float   comment '1日后收盘价',
    close_qfq_next2     float   comment '2日后收盘价',
    close_qfq_next3     float   comment '3日后收盘价',
    close_qfq_next4     float   comment '4日后收盘价',
    close_qfq_next5     float   comment '5日后收盘价',
    close_qfq_next10    float   comment '10日后收盘价',
    close_qfq_next20    float   comment '20日后收盘价',
    close_qfq_next30    float   comment '30日后收盘价',

    close_qfq_min3      float   comment '3日内最低收盘价',
    close_qfq_min5      float   comment '5日内最低收盘价',
    close_qfq_min10     float   comment '10日内最低收盘价',
    close_qfq_min20     float   comment '20日内最低收盘价',
    close_qfq_min30     float   comment '30日内最低收盘价',
    close_qfq_max3      float   comment '3日内最高收盘价',
    close_qfq_max5      float   comment '5日内最高收盘价',
    close_qfq_max10     float   comment '10日内最高收盘价',
    close_qfq_max20     float   comment '20日内最高收盘价',
    close_qfq_max30     float   comment '30日内最高收盘价',

    low_qfq             float   comment '最低价-TS前复权',
    low_qfq_min3        float   comment '3日内最低价-盘中最低价',
    low_qfq_min5        float   comment '5日内最低价-盘中最低价',
    low_qfq_min10       float   comment '10日内最低价-盘中最低价',
    low_qfq_min20       float   comment '20日内最低价-盘中最低价',
    low_qfq_min30       float   comment '30日内最低价-盘中最低价',
    high_qfq            float   comment '最高价-TS前复权',
    high_qfq_max3       float   comment '3日内最高价-盘中最高价',
    high_qfq_max5       float   comment '5日内最高价-盘中最高价',
    high_qfq_max10      float   comment '10日内最高价-盘中最高价',
    high_qfq_max20      float   comment '20日内最高价-盘中最高价',
    high_qfq_max30      float   comment '30日内最高价-盘中最高价'
)

comment 'l1-未来价格涨跌幅' stored as orc;

insert overwrite table l1.fact_stock_future_change
select
    ts_code,
    name,
    trade_date,
    close_qfq,
    lead(close_qfq, 1) over(partition by ts_code order by trade_date asc) close_qfq_next1,
    lead(close_qfq, 2) over(partition by ts_code order by trade_date asc) close_qfq_next2,
    lead(close_qfq, 3) over(partition by ts_code order by trade_date asc) close_qfq_next3,
    lead(close_qfq, 4) over(partition by ts_code order by trade_date asc) close_qfq_next4,
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
from l1.fact_stock_daily
where ts_code != 'ts_code'