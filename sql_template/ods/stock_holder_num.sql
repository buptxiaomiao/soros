
#set table_name = 'stock_holder_num'
#set unique_cols = 'ts_code,end_date'
#set cols = 'ts_code,ann_date,end_date,holder_nums,holder_num'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    ann_date        string      comment '公告日期yyyymmdd',
    end_date        string      comment '截止日期yyyymmdd',
    holder_nums     int         comment '股东户数',
    holder_num      int         comment '股东总户数（A+B）'
)  comment '股东人数'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
