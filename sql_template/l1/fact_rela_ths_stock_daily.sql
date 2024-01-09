
drop table if exists l1.fact_rela_ths_stock_daily_test;

create table if not exists l1.fact_rela_ths_stock_daily_test (

    trade_date          string      comment '交易日期',
    ts_code             string      comment 'TS代码',
    name                string      comment '股票名称',
    change_pct_stock    float       comment '股票涨跌幅',
    amount              float       comment '股票成交额',
    market              string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs               string      comment '是否是沪深股通: H/S',
    total_mv            float       comment '股票总市值（亿元）',

    ths_code            string      comment 'TS代码',
    ths_name            string      comment '名称',
    ths_type            string      comment 'I行业/N概念指数/S特色指数',
    change_pct_ths      float       comment '板块涨跌幅'
);

insert into l1.fact_rela_ths_stock_daily_test
select
    stock.trade_date,
    stock.ts_code,
    stock.name,
    stock.change_pct as change_pct_stock,
    stock.amount,
    stock.market,
    stock.is_hs,
    stock.total_mv,
    ths.ths_code,
    ths.ths_name,
    ths.ths_type,
    ths.change_pct as change_pct_ths
from l1.fact_stock_daily stock
join l1.dim_rela_ths_stock rela
    on stock.ts_code = rela.ts_code
join l1.fact_ths_daily ths
    on rela.ths_code = ths.ths_code
    and stock.trade_date = ths.trade_date
