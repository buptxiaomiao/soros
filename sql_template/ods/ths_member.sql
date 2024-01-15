
#set table_name = 'ths_member'
#set unique_cols = 'ts_code,code'
#set cols = 'ts_code,code,`name`,weight,in_date,out_date,is_new'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    code            string      comment '股票代码',
    name            string      comment '股票名称',
    `weight`        double       comment '权重',
    in_date         string      comment '纳入日期',
    out_date        string      comment '剔除日期',
    `is_new`        string      comment '是否最新Y是N否'
)  comment '同花顺概念成分'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
