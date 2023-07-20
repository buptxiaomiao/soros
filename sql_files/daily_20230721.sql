

-- ddl
create table if not exists ods_incr.daily (
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

create table if not exists ods.daily like ods_incr.daily stored as orc;

-- load data
load data local inpath 'C:\Users\wjh\PycharmProjects\soros\data_files\daily_20230721*.csv' into table ods_incr.daily partition (pt_dt='2023-07-21 01:33:34');

-- dml
insert overwrite table ods.daily partition (pt_dt='2023-07-21')
select
    ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount
from (
    select
        *,
        row_number() over(partition by ts_code,trade_date order by pt_dt desc) r
    from (
        select
            *
        from ods_incr.daily
        where pt_dt >= '2023-07-21'

        union all
        select
            *
        from ods.daily
        where pt_dt = '0000-01-01'
    ) t
) tt
where r = 1
;

insert overwrite table ods.daily partition (pt_dt='0000-01-01')
select ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount from ods.daily where pt_dt = '2023-07-21';

insert overwrite table ods.daily partition (pt_dt='9999-01-01')
select ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount from ods.daily where pt_dt = '2023-07-21';

alter table ods.daily drop if exists partition(pt_dt<='2023-07-11', pt_dt>'0000-01-01' );
alter table ods_incr.daily drop if exists partition(pt_dt<='2023-07-11', pt_dt>'0000-01-01' );


