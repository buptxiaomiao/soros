
drop table if exists l1.dim_stock;
create table if not exists l1.dim_stock (
    ts_code         string      comment 'TS代码',
    name            string      comment '股票名称',
    area            string      comment '地域',
    industry        string      comment '所属行业',
    market          string      comment '市场类型（主板/创业板/科创板/CDR）',
    is_hs           string      comment '是否是沪深股通: H/S',
    main_business   string      comment '主要业务及产品',
    `close`         double       comment '最新收盘价',
    total_mv        double       comment '最新市值(亿)',
    circ_mv         double       comment '流通市值(亿)',
    list_date       string      comment '上市日期',
    website         string      comment '公司主页',
    province        string      comment '所在省份',
    city            string      comment '所在城市',
    employees       int         comment '员工人数',
    setup_date      string      comment '注册日期',
    pe              double       comment '市盈率（总市值/净利润， 亏损的PE为空）',
    pe_ttm          double       comment '市盈率（TTM，亏损的PE为空）',
    pb              double       comment '市净率（总市值/净资产）',
    ps              double       comment '市销率',
    ps_ttm          double       comment '市销率（TTM）',
    dv_ratio        double       comment '股息率 （%）',
    dv_ttm          double       comment '股息率（TTM）（%）',
    `exchange`      string      comment '交易所代码',
    list_status     string      comment '上市状态 L上市 D退市 P暂停上市',
    symbol          string      comment '股票代码',
    holder_num      int         comment '股东户数',
    holder_ann_date string      comment '股东户数公告日期',
    holder_end_date string      comment '股东户数截止日期',
    holder_per_amount_circ      double       comment '流通户均持股金额（万）=流通市值/股东户数',

    ths_industry    string      comment '同花顺行业',
    ths_concept     string      comment '同花顺概念',
    is_hs300        tinyint     comment '沪深300成分股',
    is_a50          tinyint     comment 'A50',
    is_zz500        tinyint     comment '中证500',
    is_margin       tinyint     comment '融资融券',
    is_msci         tinyint     comment 'MSCI成分股'

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
    ps_ttm,
    dv_ratio,
    dv_ttm,
    `exchange`,
    list_status,
    symbol,
    holder.holder_nums,
    holder.ann_date as holder_ann_date,
    holder.end_date as holder_end_date,
    round(t3.circ_mv * 10000.0 / cast(holder.holder_nums as float), 2) as holder_per_amount_circ,

    ths.ths_industry,
    ths.ths_concept,
    ths.is_hs300,
    ths.is_a50,
    ths.is_zz500,
    ths.is_margin,
    ths.is_msci
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
        and name != '经纬纺机'
        and name not like '%退%'
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
            ps_ttm,
            dv_ratio,
            dv_ttm,
            total_mv / 10000.0 as total_mv,
            circ_mv / 10000.0  as circ_mv,
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
            coalesce(holder_nums, holder_num) as holder_nums,
            row_number() over(partition by ts_code order by end_date desc) r
        from ods.stock_holder_num
        where pt_dt = '9999-01-01'
    ) a
    where r = 1
) holder
    on t1.ts_code = holder.ts_code

left join (
    select
        ts_code,
        concat_ws(',', collect_set(if(ths_type='I', ths_name, null))) as ths_industry,
        concat_ws(',', collect_set(if(ths_type='R' and ths_type not in (
            '沪深300样本股', '上证50样本股', '中证500成份股', '融资融券', 'MSCI概念', '标普道琼斯A股', '三季报预增',
            '上证380成份股', '注册制次新股', '新股与次新股', 'ST板块'
            ), ths_name, null))) as ths_concept,
        max(if(ths_name='沪深300样本股', 1, null)) as is_hs300,
        max(if(ths_name='上证50样本股', 1, null)) as is_a50,
        max(if(ths_name='中证500成份股', 1, null)) as is_zz500,
        max(if(ths_name='融资融券', 1, null)) as is_margin,
        max(if(ths_name='MSCI概念', 1, null)) as is_msci

    from (
        select
            a.ts_code as ths_code,
            a.name    as ths_name,
            b.code    as ts_code,
            a.type    as ths_type
        from ods.ths_index a
        join ods.ths_member b
            on a.ts_code = b.ts_code
        where 1 = 1
            and a.pt_dt = '0000-01-01'
            and b.pt_dt = '0000-01-01'
            and a.`exchange` = 'A'
            and a.type in ('I', 'N')
            and b.is_new = 'Y'
    ) t
    group by ts_code

) ths
    on t1.ts_code = ths.ts_code