
#set table_name = 'ths_daily'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,'
#set cols += '`close`,`open`,high,low,pre_close,avg_price,change,pct_change,vol,'
#set cols += 'turnover_rate,total_mv,float_mv,pe_ttm,pb_mrq'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    trade_date      string      comment '交易日期',
    `close`         double       comment '收盘点位',
    `open`          double       comment '开盘点位',
    `high`          double       comment '最高点位',
    `low`           double       comment '最低点位',
    `pre_close`     double       comment '昨日收盘点',
    `avg_price`     double       comment '平均点位',
    `change`        double       comment '涨跌点位',
    `pct_change`    double       comment '涨跌幅',
    `vol`           double       comment '成交量',
    `turnover_rate` double       comment '换手率',
    `total_mv`      double       comment '总市值',
    `float_mv`      double       comment '流通市值',
    `pe_ttm`        double       comment 'PE TTM',
    `pb_mrq`        double       comment 'PB MRQ'
)  comment '同花顺概念和行业行情'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
