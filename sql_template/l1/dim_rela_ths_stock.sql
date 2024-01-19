
drop table if exists l1.dim_rela_ths_stock;
create table if not exists l1.dim_rela_ths_stock (

    ths_code        string      comment 'TS代码',
    ths_name        string      comment '名称',
    ths_market      string      comment '市场类型A-a股票 HK-港股 US-美股',
    ths_list_date   string      comment '上市日期',
    ths_type        string      comment 'I行业/N概念指数/S特色指数',
    ths_type_str    string      comment '概念/行业/地域/特色',

    ts_code         string      comment 'TS代码',
    name            string      comment '股票名称',
    market          string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs           string      comment '是否是沪深股通: H/S',
    `close`         double       comment '最新收盘价',
    total_mv        double       comment '最新市值(亿)',
    circ_mv         double       comment '流通市值(亿)',
    pe_ttm          double       comment '市盈率（TTM，亏损的PE为空）',
    dv_ttm          double       comment '股息率（TTM）（%）',
    holder_num      int         comment '股东户数',
    holder_per_amount_circ      float       comment '流通户均持股金额（万）=流通市值/股东户数'
)  comment 'DIM-RELA同花顺概念维表'
stored as orc;

insert overwrite table l1.dim_rela_ths_stock

select
    ths.ths_code,
    ths.ths_name,
    ths.ths_market,
    ths.ths_list_date,
    ths.ths_type,
    ths.ths_type_str,
    rela.ts_code,
    stock.name,
    stock.market,
    stock.is_hs,
    stock.`close`,
    stock.total_mv,
    stock.circ_mv,
    stock.pe_ttm,
    stock.dv_ttm,
    stock.holder_num,
    stock.holder_per_amount_circ

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
        and (type in ('I', 'N') or ts_code = '883957.TI')
) ths

join (
    select
        ts_code as ths_code,
        code as ts_code,
        name as stock_name
    from ods.ths_member
    where pt_dt = '9999-01-01'
        and is_new = 'Y'
) rela
    on ths.ths_code = rela.ths_code

join l1.dim_stock stock
    on rela.ts_code = stock.ts_code

