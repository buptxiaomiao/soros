
drop table if exists l1.fact_market_amount;
create table if not exists l1.fact_market_amount (
    trade_date          string      comment '交易日期',
    market              string      comment '市场类型（主板/创业板/科创板/北交所/全市）',
    total_mv            float       comment '总市值（亿元）',
    circ_mv             float       comment '流通市值（亿元）',
    free_mv             float       comment '自由流通市值（亿元），剔除5%以上',
    amount              float       comment '成交额（亿）',
    amount_dod          float       comment '成交量环比',
    vr                  float       comment '量比',
    stock_num           int         comment '股票数',
    up_num              int         comment '上涨股票数',
    down_num            int         comment '下跌股票数'
) comment '大盘成交量'
stored as orc;

insert overwrite table l1.fact_market_amount
select
    trade_date,
    market,
    total_mv,
    circ_mv,
    free_mv,
    amount,
    round(amount / lag(amount, 1) over(partition by market order by trade_date asc) - 1, 2) as amount_dod,
    round(amount * 5 / sum(amount) over(partition by market order by trade_date asc ROWS BETWEEN 5 preceding AND 1 preceding), 2) as vr,
    stock_num,
    up_num,
    down_num
from (
    select
        trade_date,
        if(grouping(market)!=0, market, '京沪深') as market,
        sum(total_mv) as total_mv,
        sum(circ_mv) as circ_mv,
        round(sum(free_share * total_mv / total_share), 1) as free_mv,
        round(sum(amount) / 100, 0) as amount,
        count(distinct ts_code) as stock_num,
        count(if(change_pct > 0, 1, null)) as up_num,
        count(if(change_pct < 0, 1, null)) as down_num
    from l1.fact_stock_daily
    group by trade_date, market
    grouping sets(
        (trade_date),
        (trade_date, market)
    )
) t
