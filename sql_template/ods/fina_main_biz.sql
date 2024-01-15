
#set table_name = 'fina_main_biz'
#set unique_cols = 'ts_code,end_date,bz_item,bz_code'
#set cols = 'ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_flag'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    end_date        string      comment '报告期',
    bz_item         string      comment '主营业务项目',
    bz_code         string      comment 'P按产品 D按地区',
    bz_sales        double       comment '主营业务收入(元)',
    bz_profit       double       comment '主营业务利润(元)',
    bz_cost         double       comment '主营业务成本(元)',
    curr_type       string      comment '货币代码',
    update_flag     string      comment '是否更新'
)  comment '上市公司主营业务构成，分地区和产品两种方式'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
