
#set table_name = 'ths_index'
#set unique_cols = 'ts_code'
#set cols = 'ts_code,name,`exchange`,list_date,type'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    name            string      commnet '名称',
    `exchange`      string      comment '市场类型A-a股票 HK-港股 US-美股',
    list_date       string      comment '上市日期',
    `type`          string      comment 'N概念指数S特色指数'
)  comment '同花顺概念和行业'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
