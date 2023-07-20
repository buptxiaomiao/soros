
#set table_name = 'stock_basic'
#set unique_cols = 'ts_code'
#set cols = 'ts_code,symbol,name,area,industry,market,`exchange`,list_status,list_date,delist_date,is_hs'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code     string      comment 'TS代码',
    symbol      string      comment '股票代码',
    name        string      comment '股票名称',
    area        string      comment '地域',
    industry    string      comment '所属行业',
    market      string      comment '市场类型（主板/创业板/科创板/CDR）',
    `exchange`      string      comment '交易所代码',
    list_status     string      comment '上市状态 L上市 D退市 P暂停上市',
    list_date       string      comment '上市日期',
    delist_date     string      comment '退市日期',
    is_hs           string      comment '是否沪深港通标的，N否 H沪股通 S深股通'
)  comment '股票列表'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;

create table if not exists ods.${table_name} like ods_incr.${table_name} stored as orc;

-- load data
load data local inpath '${data_file_path}' into table ods_incr.${table_name} partition (pt_dt='${now}');

-- dml
insert overwrite table ods.${table_name} partition (pt_dt='${now.date}')
select
    ${cols}
from (
    select
        *,
        row_number() over(partition by ${unique_cols} order by pt_dt desc) r
    from (
        select
            *
        from ods_incr.${table_name}
        where pt_dt >= '${now.date}'

        union all
        select
            *
        from ods.${table_name}
        where pt_dt = '0000-01-01'
    ) t
) tt
where r = 1
;

insert overwrite table ods.${table_name} partition (pt_dt='0000-01-01')
select ${cols} from ods.${table_name} where pt_dt = '${now.date}';

insert overwrite table ods.${table_name} partition (pt_dt='9999-01-01')
select ${cols} from ods.${table_name} where pt_dt = '${now.date}';

alter table ods.${table_name} drop if exists partition(pt_dt<='${now.delta(10).date}', pt_dt>'0000-01-01' );
alter table ods_incr.${table_name} drop if exists partition(pt_dt<='${now.delta(10).date}', pt_dt>'0000-01-01' );


