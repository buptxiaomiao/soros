
drop table if exists l1.topic_ths_daily;
-- ddl
create table if not exists l1.topic_ths_daily (

    trade_date      string      comment '交易日期',
    ths_code        string      comment 'TS代码',
    ths_name        string      comment '名称',
    ths_type_str    string      comment '概念/行业/地域/特色',

    value_map       map<int, double>    comment '净值map,以今日key=0为基准1,昨日key=-1,明日key=1',
    change_pct_map  map<int, double>    comment '涨跌幅map',

    is_newest           tinyint             comment '是否最新'

)  comment '同花顺概念行情'
stored as orc;


insert overwrite table l1.topic_ths_daily
select
    trade_date,
    ths_code,
    ths_name,
    ths_type_str,
    map(
        1,  round(lead(`close`, 1) over(partition by ths_code order by trade_date asc) / `close`, 4),
        2,  round(lead(`close`, 2) over(partition by ths_code order by trade_date asc) / `close`, 4),
        3,  round(lead(`close`, 3) over(partition by ths_code order by trade_date asc) / `close`, 4),
        4,  round(lead(`close`, 4) over(partition by ths_code order by trade_date asc) / `close`, 4),
        5,  round(lead(`close`, 5) over(partition by ths_code order by trade_date asc) / `close`, 4),
        6,  round(lead(`close`, 6) over(partition by ths_code order by trade_date asc) / `close`, 4),
        7,  round(lead(`close`, 7) over(partition by ths_code order by trade_date asc) / `close`, 4),
        8,  round(lead(`close`, 8) over(partition by ths_code order by trade_date asc) / `close`, 4),
        9,  round(lead(`close`, 9) over(partition by ths_code order by trade_date asc) / `close`, 4),
        10,  round(lead(`close`, 10) over(partition by ths_code order by trade_date asc) / `close`, 4),
        0, 1,
        -1,  round(lag(`close`, 1) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -2,  round(lag(`close`, 2) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -3,  round(lag(`close`, 3) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -4,  round(lag(`close`, 4) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -5,  round(lag(`close`, 5) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -6,  round(lag(`close`, 6) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -7,  round(lag(`close`, 7) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -8,  round(lag(`close`, 8) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -9,  round(lag(`close`, 9) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -10,  round(lag(`close`, 10) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -11,  round(lag(`close`, 11) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -12,  round(lag(`close`, 12) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -13,  round(lag(`close`, 13) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -14,  round(lag(`close`, 14) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -15,  round(lag(`close`, 15) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -16,  round(lag(`close`, 16) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -17,  round(lag(`close`, 17) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -18,  round(lag(`close`, 18) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -19,  round(lag(`close`, 19) over(partition by ths_code order by trade_date asc) / `close`, 4),
        -20,  round(lag(`close`, 20) over(partition by ths_code order by trade_date asc) / `close`, 4)
        ) as value_map,

    map(
        1,  lead(change_pct, 1) over(partition by ths_code order by trade_date asc),
        2,  lead(change_pct, 2) over(partition by ths_code order by trade_date asc),
        3,  lead(change_pct, 3) over(partition by ths_code order by trade_date asc),
        4,  lead(change_pct, 4) over(partition by ths_code order by trade_date asc),
        5,  lead(change_pct, 5) over(partition by ths_code order by trade_date asc),
        6,  lead(change_pct, 6) over(partition by ths_code order by trade_date asc),
        7,  lead(change_pct, 7) over(partition by ths_code order by trade_date asc),
        8,  lead(change_pct, 8) over(partition by ths_code order by trade_date asc),
        9,  lead(change_pct, 9) over(partition by ths_code order by trade_date asc),
        10,  lead(change_pct, 10) over(partition by ths_code order by trade_date asc),
        0, change_pct,
        -1,  lag(change_pct, 1) over(partition by ths_code order by trade_date asc),
        -2,  lag(change_pct, 2) over(partition by ths_code order by trade_date asc),
        -3,  lag(change_pct, 3) over(partition by ths_code order by trade_date asc),
        -4,  lag(change_pct, 4) over(partition by ths_code order by trade_date asc),
        -5,  lag(change_pct, 5) over(partition by ths_code order by trade_date asc),
        -6,  lag(change_pct, 6) over(partition by ths_code order by trade_date asc),
        -7,  lag(change_pct, 7) over(partition by ths_code order by trade_date asc),
        -8,  lag(change_pct, 8) over(partition by ths_code order by trade_date asc),
        -9,  lag(change_pct, 9) over(partition by ths_code order by trade_date asc),
        -10,  lag(change_pct, 10) over(partition by ths_code order by trade_date asc),
        -11,  lag(change_pct, 11) over(partition by ths_code order by trade_date asc),
        -12,  lag(change_pct, 12) over(partition by ths_code order by trade_date asc),
        -13,  lag(change_pct, 13) over(partition by ths_code order by trade_date asc),
        -14,  lag(change_pct, 14) over(partition by ths_code order by trade_date asc),
        -15,  lag(change_pct, 15) over(partition by ths_code order by trade_date asc),
        -16,  lag(change_pct, 16) over(partition by ths_code order by trade_date asc),
        -17,  lag(change_pct, 17) over(partition by ths_code order by trade_date asc),
        -18,  lag(change_pct, 18) over(partition by ths_code order by trade_date asc),
        -19,  lag(change_pct, 19) over(partition by ths_code order by trade_date asc),
        -20,  lag(change_pct, 20) over(partition by ths_code order by trade_date asc)
        ) as change_pct_map,

    if(rank() over(partition by ths_code order by trade_date desc) = 1, 1, null) as is_newest
from l1.fact_ths_daily
