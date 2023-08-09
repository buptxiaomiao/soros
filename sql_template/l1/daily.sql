
-- ddl
create table if not exists l1.daily (
    ts_code             string      comment 'TS代码',
    name                string      comment '股票名称',
    industry            string      comment '股票行业',
    trade_date          string      comment '交易日期',

    total_mv            float       comment '总市值（亿元）',
    total_mv_wan        float       comment '总市值（万元）'
    amount              float       comment '成交额（万元）',
    turnover_rate       float       comment '换手率（%）',
    turnover_rate_f     float       comment '换手率（自由流通股）',
    change_pct          float       comment '涨跌百分比',

    open_qfq            float       comment '开盘价-前复权',
    high_qfq            float       comment '最高价-前复权',
    low_qfq             float       comment '最低价-前复权',
    close_qfq           float       comment '收盘价-前复权',
    preclose_qfq        float       comment '昨收假-前复权',
    vol_qfq             float       comment '成交量（手）-前复权',
    change_qfq          float       comment '涨跌额-前复权',

    open_real           float       comment '开盘价-未复权',
    high_real           float       comment '最高价-未复权',
    low_real            float       comment '最低价-未复权',
    close_real          float       comment '收盘价-未复权',
    preclose_real       float       comment '昨收价（前复权）-当日看',
    vol_real            float       comment '成交量（手）-未复权',
    change_real         float       comment '涨跌额-未复权'
)  comment '日线行情'
stored as orc;


insert overwrite table l1.daily
select
    t1.ts_code,
    t3.name,
    t3.industry,
    t1.trade_date,
    t2.total_mv,
    t2.total_mv_wan,
    t2.amount,
    t2.turnover_rate,
    t2.turnober_rate_f,
    t1.change_pct,


from (
    select *
    from ods.daily
    where pt_dt = '9999-01-01'
) t1

left join (
    select *
    from ods.daily_basic
    where pt_dt = '9999-01-01'
) t2
    on t1.ts_code = t2.ts_code
    and t1.trade_date = t3.trade_date

left join l1.dim_stock dim_stock
    on t1.ts_code = dim_stock.ts_code

left join (
    select
        ts_code,
        trade_date,
        adj_factor
    from ods.adj_factor
    where pt_dt = '9999-01-01'
) adj
    on t1.ts_code = adj.ts_code
    and t1.trade_date = adj.trade_date
