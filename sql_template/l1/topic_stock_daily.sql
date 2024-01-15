
drop table if exists l1.topic_stock_daily;
-- ddl
create table if not exists l1.topic_stock_daily (
    ts_code             string      comment 'TS代码',
    name                string      comment '股票名称',
    market              string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs               string      comment '是否是沪深股通: H/S',
    trade_date          string      comment '交易日期',

    total_mv            double       comment '总市值（亿元）',
    circ_mv             double       comment '流通市值（亿元）',
    close_qfq           double      comment '收盘价-qfq',
    change_pct          double      comment '涨跌幅',
    pe_ttm              double       comment '市盈率（TTM，亏损的PE为空）',
    pe_extra            map<string, double>  comment '', -- pe pe_ttm pb ps ps_ttm dv_ratio dv_ttm

    value_map           map<int, double>     comment '-20~10',

    amount              float       comment '成交额（万元）',
    amount_map          map<int, double>     comment '-20~10'
--                                                 ,
-- --     turnover_rate       float       comment '换手率（%）',
-- --     turnover_rate_f     float       comment '换手率（自由流通股）',
--     change_pct          float       comment '涨跌百分比',
--     change_pct_map      map<int, float>     comment '-20~10',
--
--     amount_dod          float       comment '成交额环比',
--     amount_dod_map      map<int, float>     comment '-20~10',
--     up_or_down          string      comment 'up上涨/down下跌/0平盘',
--     up_or_down_map      map<int, float>     comment '-20~10',
--     red_or_green        string      comment 'red红柱/green绿柱/+十字星',
--     red_or_green_map    map<int, float>     comment '-20~10',
--
--     candle_up_rate      float       comment '上影线占比',
--     candle_solid_rate   float       comment '蜡烛实体占比',
--     candle_down_rate    float       comment '下影线占比',
--
--     volume_ratio        float       comment '量比',
--
--     close_qfq           float       comment '收盘价-前复权',
--     close_qfq_map       map<int, float>     comment '-20~10',
--     qfq_extra           map<int, float>     comment '-20~10',
-- --     open_qfq            float       comment '开盘价-前复权',
-- --     high_qfq            float       comment '最高价-前复权',
-- --     low_qfq             float       comment '最低价-前复权',
-- --     preclose_qfq        float       comment '昨收假-前复权',
-- --     vol_qfq             float       comment '成交量（手）-前复权',
-- --     change_qfq          float       comment '涨跌额-前复权',
--     real_extra          map<int, float>     comment '-20~10',
--
-- --     open_real           float       comment '开盘价-未复权',
-- --     high_real           float       comment '最高价-未复权',
-- --     low_real            float       comment '最低价-未复权',
-- --     close_real          float       comment '收盘价-未复权',
-- --     preclose_real       float       comment '昨收价（前复权）-当日看',
-- --     vol_real            float       comment '成交量（手）-未复权',
-- --     change_real         float       comment '涨跌额-未复权'
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
    map('pe', pe, 'pb', pb, 'ps', ps, 'ps_ttm', ps_ttm, 'dv_ratio', dv_ratio, 'dv_ttm', dv_ttm) as pe_extra,
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
    amount,
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
        -1,  lag(close_qfq, 1) over(partition by ts_code order by trade_date asc),
        -2,  lag(close_qfq, 2) over(partition by ts_code order by trade_date asc),
        -3,  lag(close_qfq, 3) over(partition by ts_code order by trade_date asc),
        -4,  lag(close_qfq, 4) over(partition by ts_code order by trade_date asc),
        -5,  lag(close_qfq, 5) over(partition by ts_code order by trade_date asc),
        -6,  lag(close_qfq, 6) over(partition by ts_code order by trade_date asc),
        -7,  lag(close_qfq, 7) over(partition by ts_code order by trade_date asc),
        -8,  lag(close_qfq, 8) over(partition by ts_code order by trade_date asc),
        -9,  lag(close_qfq, 9) over(partition by ts_code order by trade_date asc),
        -10,  lag(close_qfq, 10) over(partition by ts_code order by trade_date asc),
        -11,  lag(close_qfq, 11) over(partition by ts_code order by trade_date asc),
        -12,  lag(close_qfq, 12) over(partition by ts_code order by trade_date asc),
        -13,  lag(close_qfq, 13) over(partition by ts_code order by trade_date asc),
        -14,  lag(close_qfq, 14) over(partition by ts_code order by trade_date asc),
        -15,  lag(close_qfq, 15) over(partition by ts_code order by trade_date asc),
        -16,  lag(close_qfq, 16) over(partition by ts_code order by trade_date asc),
        -17,  lag(close_qfq, 17) over(partition by ts_code order by trade_date asc),
        -18,  lag(close_qfq, 18) over(partition by ts_code order by trade_date asc),
        -19,  lag(close_qfq, 19) over(partition by ts_code order by trade_date asc),
        -20,  lag(close_qfq, 20) over(partition by ts_code order by trade_date asc)
        ) as amount_map
from l1.fact_stock_daily
