
#set table_name = 'adj_factor'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,adj_factor'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    trade_date      string      comment '交易日期',
    adj_factor      double       comment '复权因子'
)  comment '复权因子'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
