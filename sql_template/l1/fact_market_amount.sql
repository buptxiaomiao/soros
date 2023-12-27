
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
    down_num            int         comment '下跌股票数',

    change_pct_median       float   comment '涨跌中位数',
    net_in_gte_lg_amount    float   comment '主力净流入（亿）（>20W）',
    net_in_md_amount        float   comment '中单净流入（亿）（5-20W）',
    net_in_sm_amount        float   comment '小单净流入（亿）（<5W）',
    net_in_amount           float   comment '净流入（亿）'
) comment '大盘成交量'
stored as orc;

insert overwrite table l1.fact_market_amount
select
    a.trade_date,
    a.market,
    total_mv,
    circ_mv,
    free_mv,
    amount,
    round(amount / lag(amount, 1) over(partition by a.market order by a.trade_date asc) - 1, 2) as amount_dod,
    round(amount * 5 / sum(amount) over(partition by a.market order by a.trade_date asc ROWS BETWEEN 5 preceding AND 1 preceding), 2) as vr,
    stock_num,
    up_num,
    down_num,
    change_pct_median,
    net_in_gte_lg_amount,
    net_in_md_amount,
    net_in_sm_amount,
    net_in_amount
    -- todo 北向资金
from (
    select
        trade_date,
        if(grouping(market)!=0, market, '京沪深') as market,
        sum(total_mv) as total_mv,
        sum(circ_mv) as circ_mv,
        round(sum(free_share * total_mv / total_share), 1) as free_mv,
        round(sum(amount) / 10000, 0) as amount,
        count(distinct ts_code) as stock_num,
        count(if(change_pct > 0, 1, null)) as up_num,
        count(if(change_pct < 0, 1, null)) as down_num,
        percentile(cast(change_pct * 100 as bigint), 0.5) / 100.0 as change_pct_median
    from l1.fact_stock_daily
    group by trade_date, market
    grouping sets(
        (trade_date),
        (trade_date, market)
    )
) a
left join (
    select
        trade_date,
        if(grouping(market)!=0, market, '京沪深') as market,
        sum(net_in_gte_lg_amount) / 10000.0 as net_in_gte_lg_amount,
        sum(buy_md_amount - sell_md_amount) / 10000.0 as net_in_md_amount,
        sum(buy_sm_amount - sell_sm_amount) / 10000.0 as net_in_sm_amount,
        sum(net_in_amount) / 10000.0 as net_in_amount
    from l1.fact_stock_money_flow
    group by trade_date, market
    grouping sets(
        (trade_date),
        (trade_date, market)
    )
) b
on a.trade_date = b.trade_date
and a.market = b.market

