
drop table if exists l1.fact_ths_daily;
-- ddl
create table if not exists l1.fact_ths_daily (

    trade_date      string      comment '交易日期',
    ths_code        string      comment 'TS代码',
    ths_name        string      comment '名称',
    ths_market      string      comment '市场类型A-a股票 HK-港股 US-美股',
    ths_list_date   string      comment '上市日期',
    ths_type        string      comment 'I行业/N概念指数/S特色指数',
    ths_type_str    string      comment '概念/行业/地域/特色',

    `close`         double       comment '收盘点位',
    `open`          double       comment '开盘点位',
    `high`          double       comment '最高点位',
    `low`           double       comment '最低点位',
    `pre_close`     double       comment '昨日收盘点',
    `avg_price`     double       comment '平均点位',
    `change`        double       comment '涨跌点位',
    `change_pct`    double       comment '涨跌幅',
    `vol`           double       comment '成交量',
    `turnover_rate` double       comment '换手率',
    `total_mv`      double       comment '总市值',
    `float_mv`      double       comment '流通市值',
    `pe_ttm`        double       comment 'PE TTM',
    `pb_mrq`        double       comment 'PB MRQ'

)  comment '同花顺概念行情'
stored as orc;


insert overwrite table l1.fact_ths_daily
select
    daily.trade_date,
    dim.ths_code,
    dim.ths_name,
    dim.ths_market,
    dim.ths_list_date,
    dim.ths_type,
    dim.ths_type_str,
    daily.`close`,
    daily.`open`,
    daily.`high`,
    daily.`low`,
    daily.`pre_close`,
    daily.`avg_price`,
    daily.`change`,
    daily.`pct_change` as change_pct,
    daily.`vol`,
    daily.`turnover_rate`,
    daily.`total_mv`,
    daily.`float_mv`,
    daily.`pe_ttm`,
    daily.`pb_mrq`
from (
    select
        ts_code as ths_code,
        name as ths_name,
        `exchange` as ths_market,
        list_date as ths_list_date,
        type as ths_type,
        case when type = 'I' then '行业'
            when type = 'R' then '地域'
            when type = 'N' then '概念'
            when type = 'S' then '特色'
        end as ths_type_str
    from ods.ths_index
    where pt_dt = '9999-01-01'
        and `exchange` = 'A'
) dim
join (
    select *
    from ods.ths_daily
    where pt_dt = '9999-01-01'
) daily
    on dim.ths_code = daily.ts_code
