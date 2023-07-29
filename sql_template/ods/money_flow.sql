
#set table_name = 'money_flow'
#set unique_cols = 'ts_code,trade_date'
#set cols = 'ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,'
#set cols += 'buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,'
#set cols += 'buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,'
#set cols += 'buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,'
#set cols += 'net_mf_vol,net_mf_amount,trade_count'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code             string      comment 'TS代码',
    trade_date          string      comment '交易日期',

    buy_sm_vol          int         comment '小单买入量（手）',
    buy_sm_amount       float       comment '小单买入金额（万元）',
    sell_sm_vol         int         comment '小单卖出量（手）',
    sell_sm_amount      float       comment '小单卖出金额（万元）',

    buy_md_vol          int         comment '中单买入量（手）',
    buy_md_amount       float       comment '中单买入金额（万元）',
    sell_md_vol         int         comment '中单卖出量（手）',
    sell_md_amount      float       comment '中单卖出金额（万元）',

    buy_lg_vol          int         comment '大单买入量（手）',
    buy_lg_amount       float       comment '大单买入金额（万元）',
    sell_lg_vol         int         comment '大单卖出量（手）',
    sell_lg_amount      float       comment '大单卖出金额（万元）',

    buy_elg_vol         int         comment '特大单买入量（手）',
    buy_elg_amount      float       comment '特大单买入金额（万元）',
    sell_elg_vol        int         comment '特大单卖出量（手）',
    sell_elg_amount     float       comment '特大单卖出金额（万元）',

    net_mf_vol          int       comment '净流入量（手）',
    net_mf_amount       float       comment '净流入额（万元）',
    trade_count         int       comment '交易笔数'
)  comment '个股资金流向 小单：5万以下 中单：5～20万 大单：20～100万 特大单：>=100万'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
