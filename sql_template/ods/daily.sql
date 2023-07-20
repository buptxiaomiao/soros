
#set table_name = 'daily'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    trade_date      string      comment '交易日期',
    `open`          float       comment '开盘价',
    high            float       comment '最高价',
    low             float       comment '最低价',
    `close`         float       comment '收盘价',
    pre_close       float       comment '昨收价(前复权)',
    `change`        float       comment '涨跌额',
    pct_chg         float       comment '涨跌幅 （未复权，如果是复权请用 通用行情接口 ）',
    vol             float       comment '成交量 （手）',
    amount          float       comment '成交额 （千元）'
)  comment '日线行情'
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


