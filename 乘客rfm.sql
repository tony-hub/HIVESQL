create table service_security.p_rfm
(
pas_id bigint comment'乘客ID',
city_name string comment'城市的名称',
city_type string comment '城市的类型',
pas_r_value int comment'最近一次成单距离统计日期天数',
pas_f_value int comment'截止统计日期，用户在91天内的成单数',
pas_m_value int comment'截止统计日期，用户在91天内的成单的单均应付',
pas_rfm_type_fazhan	 tinyint comment'乘客rfm类别是否为发展',
pas_rfm_type_high	 tinyint comment'乘客rfm类别是否为高价值',
pas_rfm_type_liushi	 tinyint comment'乘客rfm类别是否为流失用户',
pas_rfm_type_low	 tinyint comment'乘客rfm类别是否为低价值',
pas_rfm_type_new     tinyint comment'乘客rfm类别是否为新用户',
pas_rfm_type_qianli  tinyint comment'乘客rfm类别是否为潜力',
pas_rfm_type_weixi   tinyint comment'乘客rfm类别是否为维系'
)
partitioned by(
`year` string,
`month` string,
`day` string
)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
location 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/sex_complaint_y'
insert overwrite table service_security.p_rfm
partition(year,month,day)
select
a1.pid,
a1.city_name,
a1.city_type,
a1.passenger_r_value,
a1.passenger_f_value,
a1.passenger_m_value,
a1.pas_rfm_type_fazhan,
a2.pas_rfm_type_high,
a3.pas_rfm_type_liushi,
a4.pas_rfm_type_low,
a5.pas_rfm_type_new,
a6.pas_rfm_type_qianli,
a7.pas_rfm_type_weixi,
'2017' as year,
'06' as month,
'30' as day
from
(select 
pid,city_name,city_type,passenger_r_value,passenger_f_value,passenger_m_value,
case when trim(user_level)='发展' then 1 else 0 end as pas_rfm_type_fazhan 
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a1
join
(
select pid, case when trim(user_level)='高价值' then 1 else 0 end as pas_rfm_type_high
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a2
on a1.pid=a2.pid
join
(
select pid, case when trim(user_level)='流失用户' then 1 else 0 end as pas_rfm_type_liushi
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a3
on a1.pid=a3.pid
join
(
select 
pid, case when trim(user_level)='低价值' then 1 else 0 end as pas_rfm_type_low 
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a4
on a1.pid=a4.pid
join
(
select pid, case when trim(user_level)='新用户' then 1 else 0 end as pas_rfm_type_new
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a5
on a1.pid=a5.pid
join
(
select 
pid, case when trim(user_level)='潜力' then 1 else 0 end as  pas_rfm_type_qianli
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a6
on a1.pid=a6.pid
join
( 
select pid, case when trim(user_level)='维系' then 1 else 0 end as  pas_rfm_type_weixi
from  g_bi.fast_passenger_rfm_tag where concat_ws('-',year,month,day)='2017-06-30'
)a7
on a1.pid=a7.pid;

select count(*),concat('-',year,month,day) from  g_bi.fast_passenger_rfm_tag  where concat_ws('-',year,month,day)>='2017-07-28' group by year,month,day