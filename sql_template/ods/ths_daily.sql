
#set table_name = 'ths_daily'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,'
#set cols += '`close`,`open`,high,low,pre_close,avg_price,change,pct_change,vol,'
#set cols += 'turnover_rate,total_mv,float_mv,pe_ttm,pb_mrq'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    trade_date      string      comment '交易日期',
    `close`         float       comment '收盘点位',
    `open`          float       comment '开盘点位',
    `high`          float       comment '最高点位',
    `low`           float       comment '最低点位',
    `pre_close`     float       comment '昨日收盘点',
    `avg_price`     float       comment '平均点位',
    `change`        float       comment '涨跌点位',
    `pct_change`    float       comment '涨跌幅',
    `vol`           float       comment '成交量',
    `turnover_rate` float       comment '换手率',
    `total_mv`      float       comment '总市值',
    `float_mv`      float       comment '流通市值',
    `pe_ttm`        float       comment 'PE TTM',
    `pb_mrq`        float       comment 'PB MRQ'
)  comment '同花顺概念和行业行情'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
