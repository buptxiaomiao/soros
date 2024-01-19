
drop table if exists l1.topic_stock_daily;
-- ddl
create table if not exists l1.topic_stock_daily (
    ts_code             string      comment 'TS代码',
    name                string      comment '股票名称',
    is_hs               string      comment '是否是沪深股通: H/S',
    trade_date          string      comment '交易日期',

    total_mv            double      comment '总市值（亿元）',
    circ_mv             double      comment '流通市值（亿元）',
    close_qfq           double      comment '收盘价-qfq',
    pe_ttm              double       comment '市盈率（TTM，亏损的PE为空）',

    value_map           map<int, double>     comment '-20~10', -- 累计，直接加减
    amount_map          map<int, double>     comment '-20~10',
    amount_dod_map      map<int, double>     comment '-20~10',
    change_pct_map      map<int, double>     comment '-20~10', -- 对比日alpha
    volume_ratio_map    map<int, double>     comment '量比',

    is_newest           tinyint             comment '是否最新'

--     up_or_down          string      comment 'up上涨/down下跌/0平盘',
--     up_or_down_map      map<int, float>     comment '-20~10',
--     red_or_green        string      comment 'red红柱/green绿柱/+十字星',
--     red_or_green_map    map<int, float>     comment '-20~10',
)  comment '日线行情'
stored as orc;


insert overwrite table l1.topic_stock_daily

select
    ts_code,
    name,
    market,
    is_hs,
    trade_date,
    total_mv,
    circ_mv,
    pe_ttm,
--     map('pe', pe, 'pb', pb, 'ps', ps, 'ps_ttm', ps_ttm, 'dv_ratio', dv_ratio, 'dv_ttm', dv_ttm) as pe_extra,
    map(
        1,  round(lead(close_qfq, 1) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        2,  round(lead(close_qfq, 2) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        3,  round(lead(close_qfq, 3) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        4,  round(lead(close_qfq, 4) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        5,  round(lead(close_qfq, 5) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        6,  round(lead(close_qfq, 6) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        7,  round(lead(close_qfq, 7) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        8,  round(lead(close_qfq, 8) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        9,  round(lead(close_qfq, 9) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        10,  round(lead(close_qfq, 10) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        0, 1,
        -1,  round(lag(close_qfq, 1) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -2,  round(lag(close_qfq, 2) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -3,  round(lag(close_qfq, 3) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -4,  round(lag(close_qfq, 4) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -5,  round(lag(close_qfq, 5) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -6,  round(lag(close_qfq, 6) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -7,  round(lag(close_qfq, 7) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -8,  round(lag(close_qfq, 8) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -9,  round(lag(close_qfq, 9) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -10,  round(lag(close_qfq, 10) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -11,  round(lag(close_qfq, 11) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -12,  round(lag(close_qfq, 12) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -13,  round(lag(close_qfq, 13) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -14,  round(lag(close_qfq, 14) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -15,  round(lag(close_qfq, 15) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -16,  round(lag(close_qfq, 16) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -17,  round(lag(close_qfq, 17) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -18,  round(lag(close_qfq, 18) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -19,  round(lag(close_qfq, 19) over(partition by ts_code order by trade_date asc) / close_qfq, 4),
        -20,  round(lag(close_qfq, 20) over(partition by ts_code order by trade_date asc) / close_qfq, 4)
        ) as value_map,

    map(
        1,  lead(amount, 1) over(partition by ts_code order by trade_date asc),
        2,  lead(amount, 2) over(partition by ts_code order by trade_date asc),
        3,  lead(amount, 3) over(partition by ts_code order by trade_date asc),
        4,  lead(amount, 4) over(partition by ts_code order by trade_date asc),
        5,  lead(amount, 5) over(partition by ts_code order by trade_date asc),
        6,  lead(amount, 6) over(partition by ts_code order by trade_date asc),
        7,  lead(amount, 7) over(partition by ts_code order by trade_date asc),
        8,  lead(amount, 8) over(partition by ts_code order by trade_date asc),
        9,  lead(amount, 9) over(partition by ts_code order by trade_date asc),
        10,  lead(amount, 10) over(partition by ts_code order by trade_date asc),
        0, amount,
        -1,  lag(amount, 1) over(partition by ts_code order by trade_date asc),
        -2,  lag(amount, 2) over(partition by ts_code order by trade_date asc),
        -3,  lag(amount, 3) over(partition by ts_code order by trade_date asc),
        -4,  lag(amount, 4) over(partition by ts_code order by trade_date asc),
        -5,  lag(amount, 5) over(partition by ts_code order by trade_date asc),
        -6,  lag(amount, 6) over(partition by ts_code order by trade_date asc),
        -7,  lag(amount, 7) over(partition by ts_code order by trade_date asc),
        -8,  lag(amount, 8) over(partition by ts_code order by trade_date asc),
        -9,  lag(amount, 9) over(partition by ts_code order by trade_date asc),
        -10,  lag(amount, 10) over(partition by ts_code order by trade_date asc),
        -11,  lag(amount, 11) over(partition by ts_code order by trade_date asc),
        -12,  lag(amount, 12) over(partition by ts_code order by trade_date asc),
        -13,  lag(amount, 13) over(partition by ts_code order by trade_date asc),
        -14,  lag(amount, 14) over(partition by ts_code order by trade_date asc),
        -15,  lag(amount, 15) over(partition by ts_code order by trade_date asc),
        -16,  lag(amount, 16) over(partition by ts_code order by trade_date asc),
        -17,  lag(amount, 17) over(partition by ts_code order by trade_date asc),
        -18,  lag(amount, 18) over(partition by ts_code order by trade_date asc),
        -19,  lag(amount, 19) over(partition by ts_code order by trade_date asc),
        -20,  lag(amount, 20) over(partition by ts_code order by trade_date asc)
        ) as amount_map,
    map(
        1,  lead(amount_dod, 1) over(partition by ts_code order by trade_date asc),
        2,  lead(amount_dod, 2) over(partition by ts_code order by trade_date asc),
        3,  lead(amount_dod, 3) over(partition by ts_code order by trade_date asc),
        4,  lead(amount_dod, 4) over(partition by ts_code order by trade_date asc),
        5,  lead(amount_dod, 5) over(partition by ts_code order by trade_date asc),
        6,  lead(amount_dod, 6) over(partition by ts_code order by trade_date asc),
        7,  lead(amount_dod, 7) over(partition by ts_code order by trade_date asc),
        8,  lead(amount_dod, 8) over(partition by ts_code order by trade_date asc),
        9,  lead(amount_dod, 9) over(partition by ts_code order by trade_date asc),
        10,  lead(amount_dod, 10) over(partition by ts_code order by trade_date asc),
        0, amount,
        -1,  lag(amount_dod, 1) over(partition by ts_code order by trade_date asc),
        -2,  lag(amount_dod, 2) over(partition by ts_code order by trade_date asc),
        -3,  lag(amount_dod, 3) over(partition by ts_code order by trade_date asc),
        -4,  lag(amount_dod, 4) over(partition by ts_code order by trade_date asc),
        -5,  lag(amount_dod, 5) over(partition by ts_code order by trade_date asc),
        -6,  lag(amount_dod, 6) over(partition by ts_code order by trade_date asc),
        -7,  lag(amount_dod, 7) over(partition by ts_code order by trade_date asc),
        -8,  lag(amount_dod, 8) over(partition by ts_code order by trade_date asc),
        -9,  lag(amount_dod, 9) over(partition by ts_code order by trade_date asc),
        -10,  lag(amount_dod, 10) over(partition by ts_code order by trade_date asc),
        -11,  lag(amount_dod, 11) over(partition by ts_code order by trade_date asc),
        -12,  lag(amount_dod, 12) over(partition by ts_code order by trade_date asc),
        -13,  lag(amount_dod, 13) over(partition by ts_code order by trade_date asc),
        -14,  lag(amount_dod, 14) over(partition by ts_code order by trade_date asc),
        -15,  lag(amount_dod, 15) over(partition by ts_code order by trade_date asc),
        -16,  lag(amount_dod, 16) over(partition by ts_code order by trade_date asc),
        -17,  lag(amount_dod, 17) over(partition by ts_code order by trade_date asc),
        -18,  lag(amount_dod, 18) over(partition by ts_code order by trade_date asc),
        -19,  lag(amount_dod, 19) over(partition by ts_code order by trade_date asc),
        -20,  lag(amount_dod, 20) over(partition by ts_code order by trade_date asc)
        ) as amount_dod_map,

    map(
        1,  lead(change_pct, 1) over(partition by ts_code order by trade_date asc),
        2,  lead(change_pct, 2) over(partition by ts_code order by trade_date asc),
        3,  lead(change_pct, 3) over(partition by ts_code order by trade_date asc),
        4,  lead(change_pct, 4) over(partition by ts_code order by trade_date asc),
        5,  lead(change_pct, 5) over(partition by ts_code order by trade_date asc),
        6,  lead(change_pct, 6) over(partition by ts_code order by trade_date asc),
        7,  lead(change_pct, 7) over(partition by ts_code order by trade_date asc),
        8,  lead(change_pct, 8) over(partition by ts_code order by trade_date asc),
        9,  lead(change_pct, 9) over(partition by ts_code order by trade_date asc),
        10,  lead(change_pct, 10) over(partition by ts_code order by trade_date asc),
        0, change_pct,
        -1,  lag(change_pct, 1) over(partition by ts_code order by trade_date asc),
        -2,  lag(change_pct, 2) over(partition by ts_code order by trade_date asc),
        -3,  lag(change_pct, 3) over(partition by ts_code order by trade_date asc),
        -4,  lag(change_pct, 4) over(partition by ts_code order by trade_date asc),
        -5,  lag(change_pct, 5) over(partition by ts_code order by trade_date asc),
        -6,  lag(change_pct, 6) over(partition by ts_code order by trade_date asc),
        -7,  lag(change_pct, 7) over(partition by ts_code order by trade_date asc),
        -8,  lag(change_pct, 8) over(partition by ts_code order by trade_date asc),
        -9,  lag(change_pct, 9) over(partition by ts_code order by trade_date asc),
        -10,  lag(change_pct, 10) over(partition by ts_code order by trade_date asc),
        -11,  lag(change_pct, 11) over(partition by ts_code order by trade_date asc),
        -12,  lag(change_pct, 12) over(partition by ts_code order by trade_date asc),
        -13,  lag(change_pct, 13) over(partition by ts_code order by trade_date asc),
        -14,  lag(change_pct, 14) over(partition by ts_code order by trade_date asc),
        -15,  lag(change_pct, 15) over(partition by ts_code order by trade_date asc),
        -16,  lag(change_pct, 16) over(partition by ts_code order by trade_date asc),
        -17,  lag(change_pct, 17) over(partition by ts_code order by trade_date asc),
        -18,  lag(change_pct, 18) over(partition by ts_code order by trade_date asc),
        -19,  lag(change_pct, 19) over(partition by ts_code order by trade_date asc),
        -20,  lag(change_pct, 20) over(partition by ts_code order by trade_date asc)
        ) as change_pct_map,

    map(
        1,  lead(volume_ratio, 1) over(partition by ts_code order by trade_date asc),
        2,  lead(volume_ratio, 2) over(partition by ts_code order by trade_date asc),
        3,  lead(volume_ratio, 3) over(partition by ts_code order by trade_date asc),
        4,  lead(volume_ratio, 4) over(partition by ts_code order by trade_date asc),
        5,  lead(volume_ratio, 5) over(partition by ts_code order by trade_date asc),
        6,  lead(volume_ratio, 6) over(partition by ts_code order by trade_date asc),
        7,  lead(volume_ratio, 7) over(partition by ts_code order by trade_date asc),
        8,  lead(volume_ratio, 8) over(partition by ts_code order by trade_date asc),
        9,  lead(volume_ratio, 9) over(partition by ts_code order by trade_date asc),
        10,  lead(volume_ratio, 10) over(partition by ts_code order by trade_date asc),
        0, volume_ratio,
        -1,  lag(volume_ratio, 1) over(partition by ts_code order by trade_date asc),
        -2,  lag(volume_ratio, 2) over(partition by ts_code order by trade_date asc),
        -3,  lag(volume_ratio, 3) over(partition by ts_code order by trade_date asc),
        -4,  lag(volume_ratio, 4) over(partition by ts_code order by trade_date asc),
        -5,  lag(volume_ratio, 5) over(partition by ts_code order by trade_date asc),
        -6,  lag(volume_ratio, 6) over(partition by ts_code order by trade_date asc),
        -7,  lag(volume_ratio, 7) over(partition by ts_code order by trade_date asc),
        -8,  lag(volume_ratio, 8) over(partition by ts_code order by trade_date asc),
        -9,  lag(volume_ratio, 9) over(partition by ts_code order by trade_date asc),
        -10,  lag(volume_ratio, 10) over(partition by ts_code order by trade_date asc),
        -11,  lag(volume_ratio, 11) over(partition by ts_code order by trade_date asc),
        -12,  lag(volume_ratio, 12) over(partition by ts_code order by trade_date asc),
        -13,  lag(volume_ratio, 13) over(partition by ts_code order by trade_date asc),
        -14,  lag(volume_ratio, 14) over(partition by ts_code order by trade_date asc),
        -15,  lag(volume_ratio, 15) over(partition by ts_code order by trade_date asc),
        -16,  lag(volume_ratio, 16) over(partition by ts_code order by trade_date asc),
        -17,  lag(volume_ratio, 17) over(partition by ts_code order by trade_date asc),
        -18,  lag(volume_ratio, 18) over(partition by ts_code order by trade_date asc),
        -19,  lag(volume_ratio, 19) over(partition by ts_code order by trade_date asc),
        -20,  lag(volume_ratio, 20) over(partition by ts_code order by trade_date asc)
        ) as volume_ratio_map,


    if(rank() over(partition by ts_code order by trade_date desc) = 1, 1, null) as is_newest

from l1.fact_stock_daily
