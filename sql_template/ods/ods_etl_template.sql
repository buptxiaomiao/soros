
-- ods
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


