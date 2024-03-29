
drop table if exists l1.fact_stock_daily;
-- ddl
create table if not exists l1.fact_stock_daily (
    ts_code             string      comment 'TS代码',
    name                string      comment '股票名称',
    industry            string      comment '股票行业',
    market              string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs               string      comment '是否是沪深股通: H/S',
    trade_date          string      comment '交易日期',

    total_mv            double       comment '总市值（亿元）',
    circ_mv             double       comment '流通市值（亿元）',
    total_share         double       comment '总股本（万股）',
    float_share         double       comment '流通股本（万股）',
    free_share          double       comment '自由流通股本（万股），剔除5%以上',

    pe                  double       comment '市盈率（总市值/净利润， 亏损的PE为空）',
    pe_ttm              double       comment '市盈率（TTM，亏损的PE为空）',
    pb                  double       comment '市净率（总市值/净资产）',
    ps                  double       comment '市销率',
    ps_ttm              double       comment '市销率（TTM）',
    dv_ratio            double       comment '股息率 （%）',
    dv_ttm              double       comment '股息率（TTM）（%）',

    amount              double       comment '成交额（万元）',
    turnover_rate       double       comment '换手率（%）',
    turnover_rate_f     double       comment '换手率（自由流通股）',
    change_pct          double       comment '涨跌百分比',
    zhenfu              double       comment '',

    amount_dod          double       comment '成交额环比',
    up_or_down          string      comment 'up上涨/down下跌/0平盘',
    red_or_green        string      comment 'red红柱/green绿柱/-',

    candle_up_rate      double       comment '上影线占比',
    candle_solid_rate   double       comment '蜡烛实体占比',
    candle_down_rate    double       comment '下影线占比',

    volume_ratio        double       comment '量比',

    open_qfq            double       comment '开盘价-前复权',
    high_qfq            double       comment '最高价-前复权',
    low_qfq             double       comment '最低价-前复权',
    close_qfq           double       comment '收盘价-前复权',
    preclose_qfq        double       comment '昨收假-前复权',
    vol_qfq             double       comment '成交量（手）-前复权',
    change_qfq          double       comment '涨跌额-前复权',

    open_real           double       comment '开盘价-未复权',
    high_real           double       comment '最高价-未复权',
    low_real            double       comment '最低价-未复权',
    close_real          double       comment '收盘价-未复权',
    preclose_real       double       comment '昨收价（前复权）-当日看',
    vol_real            double       comment '成交量（手）-未复权',
    change_real         double       comment '涨跌额-未复权'
)  comment '日线行情'
stored as orc;


insert overwrite table l1.fact_stock_daily
select
    t1.ts_code,
    dim_stock.name,
    dim_stock.industry,
    dim_stock.market,
    dim_stock.is_hs,
    t1.trade_date,

    round(t2.total_mv / 10000, 1) as total_mv,
    round(t2.circ_mv / 10000, 1) as circ_mv,
    t2.total_share,
    t2.float_share,
    t2.free_share,

    t2.pe,
    t2.pe_ttm,
    t2.pb,
    t2.ps,
    t2.ps_ttm,
    t2.dv_ratio,
    t2.dv_ttm,

    round(t1.amount / 10, 0) as amount,
    t2.turnover_rate,
    t2.turnover_rate_f,
    t1.pct_chg as change_pct,
    (t1.close - t1.open)/t1.pre_close as zhenfu,

    round(t1.amount / lag(t1.amount,1) over(partition by t1.ts_code order by t1.trade_date asc) - 1, 2) as amount_dod,
    case when t1.pct_chg > 0 then 'up'
        when t1.pct_chg < 0 then 'down'
        else '0' end as up_or_down,
    case when t1.close > t1.open then 'red'
        when t1.close < t1.open then 'green'
        else '-' end as red_or_green,

    if(t1.high - t1.low > 0, round((t1.high - greatest(t1.close, t1.open)) / (t1.high - t1.low), 2), 0) as  candle_up_ratio,
    if(t1.high - t1.low > 0, round(abs(t1.close - t1.open) / (t1.high - t1.low), 2), 0) as  candle_solid_ratio,
    if(t1.high - t1.low > 0, round((least(t1.close, t1.open) - t1.low) / (t1.high - t1.low), 2), 0) as  candle_down_ratio,

    t2.volume_ratio,

    round(t1.open * adj.factor, 2) as open_qfq,
    round(t1.high * adj.factor, 2) as high_qfq,
    round(t1.low * adj.factor, 2) as low_qfq,
    round(t1.close * adj.factor, 2) as close_qfq,
    round(t1.pre_close * adj.factor, 2) as preclose_qfq,
    round(t1.vol * adj.factor, 2) as vol_qfq,
    round(t1.change * adj.factor, 2) as change_qfq,

    t1.open as open_real,
    t1.high as high_real,
    t1.low as low_real,
    t1.close as close_real,
    t1.pre_close as preclose_real,
    t1.vol as vol_real,
    t1.change as change_real

from (
    select *
    from ods.daily
    where pt_dt = '9999-01-01'
        and ts_code != 'ts_code'
        and trade_date != 'trade_date'
) t1

left join (
    select *
    from ods.daily_basic
    where pt_dt = '9999-01-01'
) t2
    on t1.ts_code = t2.ts_code
    and t1.trade_date = t2.trade_date

left join l1.dim_stock dim_stock
    on t1.ts_code = dim_stock.ts_code

left join (
    select
        a.ts_code,
        a.trade_date,
        if(a.adj_factor = 1, 1, a.adj_factor / b.last_factor) factor
    from (
        select ts_code,
                trade_date,
                adj_factor
        from ods.adj_factor
        where pt_dt = '9999-01-01'
     ) a left join (
         select
            ts_code,
            max(adj_factor) last_factor
         from ods.adj_factor
         where pt_dt = '9999-01-01'
         group by
            ts_code
    ) b
    on a.ts_code = b.ts_code
) adj
    on t1.ts_code = adj.ts_code
    and t1.trade_date = adj.trade_date
