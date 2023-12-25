
drop table if exists l1.fact_stock_holder_log;
create table if not exists l1.fact_stock_holder_log (
    ts_code         string      comment 'TS代码',
    name            string      comment '股票名称',
    holder_num      bigint      comment '股东户数',
    holder_ann_date string      comment '股东户数公告日期',
    holder_end_date string      comment '股东户数截止日期',
    total_mv        float       comment '最新市值(亿)', -- 最新市值
    circ_mv         float       comment '流通市值(亿)', -- 最新流通市值
--     per_amount      float       comment '流通户均持股金额（万）=流通市值/持股人数',
    `close`         float       comment '最新收盘价',
    area            string      comment '地域',
    industry        string      comment '所属行业',
    market          string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs           string      comment '是否是沪深股通: H/S',
    is_newest       tinyint     comment '是否最新'
)  comment '股票股东户数'
stored as orc;

insert overwrite table l1.fact_stock_holder_log
select
    dim.ts_code,
    dim.name,
    holder.holder_nums,
    holder.ann_date,
    holder.end_date,
    total_mv,
    circ_mv,
    `close`,
    area,
    industry,
    market,
    is_hs,
    if(holder.r = 1, 1, null) as is_newest
from l1.dim_stock dim
join (
    select * from (
        select
            ts_code,
            ann_date,
            end_date,
            holder_nums,
            row_number() over(partition by ts_code order by end_date desc) r
        from ods.stock_holder_num
        where pt_dt = '9999-01-01'
    ) a
) holder
    on dim.ts_code = holder.ts_code
