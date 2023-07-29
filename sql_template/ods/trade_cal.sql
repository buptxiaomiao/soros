
#set table_name = 'trade_cal'
#set unique_cols = '`exchange`,cal_date'
#set cols = '`exchange`,cal_date,is_open,pretrade_date'

-- ddl
create table if not exists ods_incr.${table_name} (
    `exchange`      string      comment '交易所代码',
    cal_date        string      comment '交易所 SSE上交所 SZSE深交所',
    is_open         string      comment '日历日期',
    pretrade_date   string      comment '上一个交易日'
)  comment '交易日历'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
