

-- ddl
create table if not exists ods_incr.daily (
    ts_code         string      comment 'TS����',
    trade_date      string      comment '��������',
    `open`          float       comment '���̼�',
    high            float       comment '��߼�',
    low             float       comment '��ͼ�',
    `close`         float       comment '���̼�',
    pre_close       float       comment '���ռ�(ǰ��Ȩ)',
    `change`        float       comment '�ǵ���',
    pct_chg         float       comment '�ǵ��� ��δ��Ȩ������Ǹ�Ȩ���� ͨ������ӿ� ��',
    vol             float       comment '�ɽ��� ���֣�',
    amount          float       comment '�ɽ��� ��ǧԪ��'
)  comment '��������'
partitioned by (
    pt_dt   string  comment '����ʱ��yyyy-mm-dd'
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


