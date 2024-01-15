
#set table_name = 'hk_hold'
#set unique_cols = 'trade_date,ts_code'
#set cols = 'code,trade_date,ts_code,name,vol,ratio,`exchange`'

-- ddl
create table if not exists ods_incr.${table_name} (
    code            string      comment '原始代码',
    trade_date      string      comment '交易日期yyyymmdd',
    ts_code         string      comment 'TS代码',
    name            string      comment '股票名称',
    vol             bigint      comment '持股数量',
    ratio           double       comment '持股占比',
    `exchange`      string      comment '类型：SH沪股通SZ深港通'
)  comment '沪深港股通持股明细'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
