
#set table_name = 'daily_basic'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,`close`,turnover_rate,turnover_rate_f,`volume_ratio`,pe,`pe_ttm`,pb,ps,ps_ttm,'
#set cols += 'dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv,limit_status'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code             string      comment 'TS代码',
    trade_date          string      comment '交易日期',
    `close`             float       comment '收盘价',
    turnover_rate       float       comment '换手率（%）',
    turnover_rate_f     float       comment '换手率（自由流通股）',
    `volume_ratio`      float       comment '量比',
    pe                  float       comment '市盈率（总市值/净利润， 亏损的PE为空）',
    `pe_ttm`            float       comment '市盈率（TTM，亏损的PE为空）',
    pb                  float       comment '市净率（总市值/净资产）',
    ps                  float       comment '市销率',
    ps_ttm              float       comment '市销率（TTM）',
    dv_ratio            float       comment '股息率 （%）',
    dv_ttm              float       comment '股息率（TTM）（%）',
    total_share         float       comment '总股本 （万股）',
    float_share         float       comment '流通股本 （万股）',
    free_share          float       comment '自由流通股本 （万）',
    total_mv            float       comment '总市值 （万元）',
    circ_mv             float       comment '流通市值（万元）',
    limit_status        float       comment '涨跌停状态'
)  comment '每日指标'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;

