
#set table_name = 'daily_basic'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,`close`,turnover_rate,turnover_rate_f,`volume_ratio`,pe,`pe_ttm`,pb,ps,ps_ttm,'
#set cols += 'dv_ratio,dv_ttm,total_share,double_share,free_share,total_mv,circ_mv,limit_status'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code             string      comment 'TS代码',
    trade_date          string      comment '交易日期',
    `close`             double       comment '收盘价',
    turnover_rate       double       comment '换手率（%）',
    turnover_rate_f     double       comment '换手率（自由流通股）',
    `volume_ratio`      double       comment '量比',
    pe                  double       comment '市盈率（总市值/净利润， 亏损的PE为空）',
    `pe_ttm`            double       comment '市盈率（TTM，亏损的PE为空）',
    pb                  double       comment '市净率（总市值/净资产）',
    ps                  double       comment '市销率',
    ps_ttm              double       comment '市销率（TTM）',
    dv_ratio            double       comment '股息率 （%）',
    dv_ttm              double       comment '股息率（TTM）（%）',
    total_share         double       comment '总股本 （万股）',
    float_share         double       comment '流通股本 （万股）',
    free_share          double       comment '自由流通股本 （万）',
    total_mv            double       comment '总市值 （万元）',
    circ_mv             double       comment '流通市值（万元）',
    limit_status        double       comment '涨跌停状态'
)  comment '每日指标'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;

