
#set table_name = 'stock_company'
#set unique_cols = 'ts_code'
#set cols = 'ts_code,`exchange`,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,business_scope,employees,main_business'

-- ddl
create table if not exists ods_incr.${table_name} (
    ts_code         string      comment 'TS代码',
    `exchange`      string      comment '交易所代码 SSE上交所 SZSE深交所',
    chairman        string      comment '法人代表',
    manager         string      comment '总经理',
    secretary       string      comment '董秘',
    reg_capital	    double      comment '注册资本',
    setup_date      string      comment '注册日期',
    province        string      comment '所在省份',
    city            string      comment '所在城市',
    introduction    string      comment '公司介绍',
    website         string      comment '公司主页',
    business_scope  string      comment '经营范围',
    employees       int         comment '员工人数',
    main_business   string      comment '主要业务及产品'
)  comment '上市公司基本信息'
partitioned by (
    pt_dt   string  comment '分区时间yyyy-mm-dd'
) row format delimited fields terminated by '\u0001' stored as textfile;
