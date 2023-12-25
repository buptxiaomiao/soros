
drop table if exists l1.dim_stock;
create table if not exists l1.dim_stock (
    ts_code         string      comment 'TS代码',
    name            string      comment '股票名称',
    area            string      comment '地域',
    industry        string      comment '所属行业',
    market          string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs           string      comment '是否是沪深股通: H/S',
    main_business   string      comment '主要业务及产品',
    `close`         float       comment '最新收盘价',
    total_mv        float       comment '最新市值(亿)',
    circ_mv         float       comment '流通市值(亿)',
    list_date       string      comment '上市日期',
    website         string      comment '公司主页',
    province        string      comment '所在省份',
    city            string      comment '所在城市',
    employees       int         comment '员工人数',
    setup_date      string      comment '注册日期',
    pe              float       comment '市盈率（总市值/净利润， 亏损的PE为空）',
    pe_ttm          float       comment '市盈率（TTM，亏损的PE为空）',
    pb              float       comment '市净率（总市值/净资产）',
    ps              float       comment '市销率',
    `exchange`      string      comment '交易所代码',
    list_status     string      comment '上市状态 L上市 D退市 P暂停上市',
    symbol          string      comment '股票代码',
    holder_num      bigint      comment '股东户数',
    holder_ann_date string      comment '股东户数公告日期',
    holder_end_date string      comment '股东户数截止日期',
    holder_per_amount_circ      float       comment '流通户均持股金额（万）=流通市值/股东户数'
)  comment '股票维表'
stored as orc;

insert overwrite table l1.dim_stock
select
    t1.ts_code,
    name,
    area,
    industry,
    market,
    is_hs,
    main_business,
    `close`,
    total_mv,
    circ_mv,
    list_date,
    website,
    province,
    city,
    employees,
    setup_date,
    pe,
    pe_ttm,
    pb,
    ps,
    `exchange`,
    list_status,
    symbol,
    holder.holder_nums,
    holder.ann_date as holder_ann_date,
    holder.end_date as holder_end_date,
    round(circ_mv * 10000 / holder.holder_nums, 2) as holder_per_amount_circ
from (
    select
        ts_code,
        symbol,
        name,
        area,
        industry,
        market,
        `exchange`,
        list_status,
        list_date,
        if(is_hs in ('H', 'S'), is_hs, null) is_hs
    from ods.stock_basic
    where pt_dt = '9999-01-01'
) t1
left join (
    select
        ts_code,
        setup_date,
        province,
        city,
        website,
        employees,
        main_business
    from ods.stock_company
    where pt_dt = '9999-01-01'
) t2
    on t1.ts_code = t2.ts_code
left join (
    select
         *
    from (
        select
            ts_code,
            trade_date,
            `close`,
            pe,
            pe_ttm,
            pb,
            ps,
            total_mv / 10000 as total_mv,
            circ_mv / 10000  as circ_mv,
            row_number() over(partition by ts_code order by trade_date desc) r
        from ods.daily_basic
        where pt_dt = '9999-01-01'
            and trade_date > replace(cast(date_sub(current_date(), 30) as string), '-', '')
    ) a where r = 1
) t3
    on t1.ts_code = t3.ts_code

left join (
    select * from (
        select
            ts_code,
            ann_date,
            end_date,
            holder_nums,
            row_number() over(partition by ts_code order by end_date desc) r
        from ods.stock_holder_num
        where pt_dt = '0000-01-01'
    ) a
    where r = 1
) holder
    on t1.ts_code = holder.ts_code