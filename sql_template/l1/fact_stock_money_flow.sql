
drop table if exists l1.fact_stock_money_flow;

create table if not exists l1.fact_stock_money_flow (
    ts_code             string      comment 'TS代码',
    trade_date          string      comment '交易日期',
    name                string      comment '股票名称',
    market              string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs               string      comment '是否是沪深股通: H/S',

    circ_mv             double       comment '流通市值（亿元）',
    close_qfq           double       comment '当日收盘价',
    change_pct          double       comment '涨跌百分比',
    volume_ratio        double       comment '量比',
    total_vol           double       comment '总成交量（手）',
    total_amount        double       comment '总成交额（万元）',
    avg_vol_per_trade   double       comment '单笔交易手数',
    net_in_gte_lg_amount   double    comment '大单及以上净流入金额（万元）',
    net_in_gte_lg_amount_last2  double   comment '近2日大单净流入金额（万元）',
    net_in_gte_lg_amount_last5  double   comment '近5日大单净流入金额（万元）',
    net_in_gte_lg_amount_last10 double   comment '近10日大单净流入金额（万元）',

    buy_sm_vol          int         comment '小单买入量（手）',
    buy_sm_amount       double       comment '小单买入金额（万元）',
    sell_sm_vol         int         comment '小单卖出量（手）',
    sell_sm_amount      double       comment '小单卖出金额（万元）',

    buy_md_vol          int         comment '中单买入量（手）',
    buy_md_amount       double       comment '中单买入金额（万元）',
    sell_md_vol         int         comment '中单卖出量（手）',
    sell_md_amount      double       comment '中单卖出金额（万元）',

    buy_lg_vol          int         comment '大单买入量（手）',
    buy_lg_amount       double       comment '大单买入金额（万元）',
    sell_lg_vol         int         comment '大单卖出量（手）',
    sell_lg_amount      double       comment '大单卖出金额（万元）',

    buy_elg_vol         int         comment '特大单买入量（手）',
    buy_elg_amount      double       comment '特大单买入金额（万元）',
    sell_elg_vol        int         comment '特大单卖出量（手）',
    sell_elg_amount     double       comment '特大单卖出金额（万元）',

    buy_gte_lg_vol      int         comment '大单及以上买入量（手）',
    but_gte_lg_amount   double       comment '大单及以上买入金额（万元）',
    sell_gte_lg_vol     int         comment '大单及以上卖出量（手）',
    sell_gte_lg_amount  double       comment '大单及以上卖出金额（万元）',
    net_in_gte_lg_vol   int         comment '大单及以上净流入量（手）',

    net_in_vol          int         comment '净流入量（手）',
    net_in_amount       double       comment '净流入额（万元）',
    trade_count         int         comment '交易笔数'
)  comment '个股资金流向 小单：5万以下 中单：5～20万 大单：20～100万 特大单：>=100万'
stored as orc;

insert overwrite table l1.fact_stock_money_flow
select
    ts_code,
    trade_date,
    name,
    market,
    is_hs,
    circ_mv,
    close_qfq,
    change_pct,
    volume_ratio,
    total_vol,
    total_amount,
    round(total_amount / total_vol, 1) as avg_vol_per_trade,

    net_in_gte_lg_amount,
    sum(net_in_gte_lg_amount) over (partition by ts_code order by trade_date ASC ROWS BETWEEN 1 preceding AND CURRENT ROW) as net_in_gte_lg_amount_last2,
    sum(net_in_gte_lg_amount) over (partition by ts_code order by trade_date ASC ROWS BETWEEN 4 preceding AND CURRENT ROW) as net_in_gte_lg_amount_last5,
    sum(net_in_gte_lg_amount) over (partition by ts_code order by trade_date ASC ROWS BETWEEN 9 preceding AND CURRENT ROW) as net_in_gte_lg_amount_last10,

    buy_sm_vol,
    buy_sm_amount,
    sell_sm_vol,
    sell_sm_amount,

    buy_md_vol,
    buy_md_amount,
    sell_md_vol,
    sell_md_amount,

    buy_lg_vol,
    buy_lg_amount,
    sell_lg_vol,
    sell_lg_amount,

    buy_elg_vol,
    buy_elg_amount,
    sell_elg_vol,
    sell_elg_amount,

    buy_gte_lg_vol,
    buy_gte_lg_amount,
    sell_gte_lg_vol,
    sell_gte_lg_amount,
    net_in_gte_lg_vol,
    net_in_vol,
    net_in_amount,
    trade_count
from (
    select
        a.ts_code,
        a.trade_date,
        stock.name,
        stock.market,
        stock.is_hs,
        daily.circ_mv,
        daily.change_pct,
        daily.volume_ratio,
        daily.close_qfq,
        (buy_sm_vol + buy_md_vol + buy_lg_vol + buy_elg_vol) as total_vol,
        (buy_sm_amount + buy_md_amount + buy_lg_amount + buy_elg_amount) as total_amount,

        buy_sm_vol,
        buy_sm_amount,
        sell_sm_vol,
        sell_sm_amount,

        buy_md_vol,
        buy_md_amount,
        sell_md_vol,
        sell_md_amount,

        buy_lg_vol,
        buy_lg_amount,
        sell_lg_vol,
        sell_lg_amount,

        buy_elg_vol,
        buy_elg_amount,
        sell_elg_vol,
        sell_elg_amount,

        (buy_lg_vol + buy_elg_vol) as buy_gte_lg_vol,
        (buy_lg_amount + buy_elg_amount) as buy_gte_lg_amount,
        (sell_lg_vol + sell_elg_vol) as sell_gte_lg_vol,
        (sell_lg_amount + sell_elg_amount) as sell_gte_lg_amount,
        (buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as net_in_gte_lg_vol,
        (buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as net_in_gte_lg_amount,

        net_mf_vol as net_in_vol,
        net_mf_amount as net_in_amount,
        trade_count
    from (
        select * from ods.money_flow
        where pt_dt = '9999-01-01'
    ) a
    left join l1.dim_stock stock
        on a.ts_code = stock.ts_code
    left join l1.fact_stock_daily daily
        on a.ts_code = daily.ts_code
        and a.trade_date = daily.trade_date
) t