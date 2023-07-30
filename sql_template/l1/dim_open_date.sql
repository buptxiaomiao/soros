
-- ddl
create table if not exists l1.dim_open_date (
    cal_date        string      comment '日期yyyymmdd',
    hive_date       string      comment '日期yyyy-mm-dd',
--     is_open         string      comment '是否开市',
    week_day        string      comment '星期1,2,3..7',
    ym              string      comment '年月yyyymm',
    y               string      comment '年yyyy',
    m               string      comment '月mm',
    d               string      comment '日dd',

    pre1_date       string      comment '上1个交易日',
    pre2_date       string      comment '上2个交易日',
    pre3_date       string      comment '上3个交易日',
    pre4_date       string      comment '上4个交易日',
    pre5_date       string      comment '上5个交易日',
    pre10_date      string      comment '上10个交易日',
    pre20_date      string      comment '上20个交易日',
    pre60_date      string      comment '上60个交易日',
    pre120_date     string      comment '上120个交易日',

    week_begin_date     string      comment '周起始日期',
    week_end_date       string      comment '周结束日期',
    month_begin_date    string      comment '月起始日期',
    month_end_date      string      comment '月结束日期',
    year_begin_date     string      comment '年起始日期',
    year_end_date       string      comment '年结束日期',

    next1_date      string      comment '后1个交易日',
    next2_date      string      comment '后2个交易日',
    next3_date      string      comment '后3个交易日',
    next4_date      string      comment '后4个交易日',
    next5_date      string      comment '后5个交易日',
    next10_date     string      comment '后10个交易日',
    next20_date     string      comment '后20个交易日',

    week_first_trade_date   string      comment '周第1个交易日',
    week_last_trade_date    string      comment '周最后1个交易日',
    month_first_trade_date  string      comment '月第1个交易日',
    month_last_trade_date   string      comment '月最后1个交易日',
    year_first_trade_date   string      comment '年第一个交易日',
    year_last_trade_date    string      comment '年最后1个交易日'
)  comment '交易日历,默认yyyymmdd'
stored as orc;

insert overwrite table l1.dim_open_date
select
    t1.cal_date,
    t1.hive_date,
    t1.week_day,
    t1.ym,
    t1.y,
    t1.m,
    t1.d,
    t2.pre1_date,
    t2.pre2_date,
    t2.pre3_date,
    t2.pre4_date,
    t2.pre5_date,
    t2.pre10_date,
    t2.pre20_date,
    t2.pre60_date,
    t2.pre120_date,
    t1.week_begin_date,
    t1.week_end_date,
    t1.month_begin_date,
    t1.month_end_date,
    t1.year_begin_date,
    t1.year_end_date,

    t2.next1_date,
    t2.next2_date,
    t2.next3_date,
    t2.next4_date,
    t2.next5_date,
    t2.next10_date,
    t2.next20_date,

    min(t1.cal_date) over(partition by t1.week_begin_date) as week_first_trade_date,
    max(t1.cal_date) over(partition by t1.week_begin_date) as week_last_trade_date,
    min(t1.cal_date) over(partition by t1.month_begin_date) as month_first_trade_date,
    max(t1.cal_date) over(partition by t1.month_begin_date) as month_last_trade_date,
    min(t1.cal_date) over(partition by t1.year_begin_date) as year_first_trade_date,
    max(t1.cal_date) over(partition by t1.year_begin_date) as year_last_trade_date

from (
    select
        cal_date,
        hive_date,
--         is_open,
        if(dayofweek(hive_date)-1>0, dayofweek(hive_date)-1,7) week_day,
        ym,
        y,
        m,
        d,
        substr(date_sub(hive_date, if(dayofweek(hive_date)-1>0, dayofweek(hive_date)-1,7)-1), 0, 11) as week_begin_date,
        substr(date_add(hive_date, 7-if(dayofweek(hive_date)-1>0, dayofweek(hive_date)-1,7)), 0, 11) as week_end_date,
        substr(trunc(hive_date, 'MM'), 0, 11) as month_begin_date,
        substr(date_sub(trunc(add_months(hive_date, 1), 'MM'), 1), 0, 11) month_end_date,
        concat(y, '-01-01') as year_begin_date,
        concat(y, '-12-31') as year_end_date
    from (
        select
            cal_date,
            concat(
                substr(cal_date, 0, 4), '-',
                substr(cal_date, 4, 2), '-',
                substr(cal_date, 6, 2)
            ) as hive_date,
            is_open,
            substr(cal_date, 0, 6) as ym,
            substr(cal_date, 0, 4) as y,
            substr(cal_date, 4, 2) as m,
            substr(cal_date, 6, 2) as d
        from ods.trade_cal
        where pt_dt = '9999-01-01'
            and is_open = 1
    ) a
) t1
left join (
    select
        cal_date,
        lag(cal_date, 1) over(partition by cal_date order by cal_date asc) pre1_date,
        lag(cal_date, 2) over(partition by cal_date order by cal_date asc) pre2_date,
        lag(cal_date, 3) over(partition by cal_date order by cal_date asc) pre3_date,
        lag(cal_date, 4) over(partition by cal_date order by cal_date asc) pre4_date,
        lag(cal_date, 5) over(partition by cal_date order by cal_date asc) pre5_date,
        lag(cal_date, 10) over(partition by cal_date order by cal_date asc) pre10_date,
        lag(cal_date, 20) over(partition by cal_date order by cal_date asc) pre20_date,
        lag(cal_date, 60) over(partition by cal_date order by cal_date asc) pre60_date,
        lag(cal_date, 120) over(partition by cal_date order by cal_date asc) pre120_date,
        lead(cal_date, 1) over(partition by cal_date order by cal_date asc) next1_date,
        lead(cal_date, 2) over(partition by cal_date order by cal_date asc) next2_date,
        lead(cal_date, 3) over(partition by cal_date order by cal_date asc) next3_date,
        lead(cal_date, 4) over(partition by cal_date order by cal_date asc) next4_date,
        lead(cal_date, 5) over(partition by cal_date order by cal_date asc) next5_date,
        lead(cal_date, 10) over(partition by cal_date order by cal_date asc) next10_date,
        lead(cal_date, 20) over(partition by cal_date order by cal_date asc) next20_date
    from ods.trade_cal
    where pt_dt = '9999-01-01'
        and is_open=1
) t2
on t1.cal_date = t2.cal_date


