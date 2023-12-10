
#set table_name = 'money_flow_hsgt'
#set unique_cols = 'trade_date'
#set cols = 'trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money'

-- ddl
create table if not exists ods_incr.${table_name} (
    trade_date          string      comment '交易日期',
    ggt_ss              float       comment '港股通（上海）',
    ggt_sz              float       comment '港股通（深圳）',
    hgt                 float       comment '沪股通',
    sgt                 float       comment '深股通',
    north_money         float       comment '北向资金',
    south_money         float       comment '南向资金'
)  comment '沪股通、深股通、港股通每日资金流向数据'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
