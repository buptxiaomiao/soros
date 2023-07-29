
#set table_name = 'daily'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,`open`,high,low,`close`,pre_close,`change`,pct_chg,vol,amount'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    trade_date      string      comment '交易日期',
    `open`          float       comment '开盘价',
    high            float       comment '最高价',
    low             float       comment '最低价',
    `close`         float       comment '收盘价',
    pre_close       float       comment '昨收价(前复权)',
    `change`        float       comment '涨跌额',
    pct_chg         float       comment '涨跌幅 （未复权，如果是复权请用 通用行情接口 ）',
    vol             float       comment '成交量 （手）',
    amount          float       comment '成交额 （千元）'
)  comment '日线行情'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
