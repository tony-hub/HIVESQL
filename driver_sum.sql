create table service_security.driver_order_info(
driver_id BIGINT COMMENT'司机ID',
one_star_orders BIGINT COMMENT'一星订单数',
two_star_orders BIGINT COMMENT'二星订单数',
three_star_orders BIGINT COMMENT '三星订单数',
four_star_orders BIGINT COMMENT '四星订单数',
five_star_orders BIGINT COMMENT '五星订单数',
punish_times BIGINT COMMENT'封禁次数'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_order_info';
==============================================================================
INSERT INTO TABLE service_security.driver_order_info
PARTITION(year,month,day)
select
driver.driver_id,
driver.one_star_orders,
driver.two_star_orders,
driver.three_star_orders,
driver.four_star_orders,
driver.five_star_orders,
(case when punish.punish_num is null then cast (0 as BIGINT)
else punish.punish_num end) as punish_num,
year,
month,
day
from
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders,
five_star_orders,year,month,day
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)='2015-09-01'
)driver
join(
select driver_id from service_security.complaint_info_com
where concat_ws('-',year,month,day)='2015-09-01'
)cic
on cic.driver_id=driver.driver_id
LEFT OUTER  JOIN
(
select driver_id,punish_num from service_security.driver_punish
where concat_ws('-',year,month,day)='2015-09-01'
)punish
on punish.driver_id=cic.driver_id
distribute by(year,month,day);

==========================================================================
#!/bin/bash
start=$1
start1=`date -d "1 day ago $start" +%Y-%m-%d`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.driver_order_info
PARTITION(year,month,day)
select
(case when cic.driver_id is null then doi.driver_id else cic.driver_id end) as driver_id,

(case when driver.one_star_orders is null then 0 else driver.one_star_orders end )+
(case when doi.one_star_orders is null then 0 else doi.one_star_orders end )as one_star_orders,

(case when driver.two_star_orders is null then 0 else driver.two_star_orders end )+
(case when doi.two_star_orders is null then 0 else doi.two_star_orders end )as two_star_orders,

(case when driver.three_star_orders is null then 0 else driver.three_star_orders end )+
(case when doi.three_star_orders is null then 0 else doi.three_star_orders end)as three_star_orders,

(case when driver.four_star_orders is null then 0 else driver.four_star_orders end)+
(case when doi.four_star_orders is null then 0 else doi.four_star_orders end)as four_star_orders,

(case when driver.five_star_orders is null then 0 else driver.five_star_orders end)+
(case when doi.five_star_orders is null then 0 else doi.five_star_orders end)as five_star_orders,

(case when punish.punish_num is null then 0 else punish.punish_num end )+
(case when doi.forbidden_num is null then 0 else doi.forbidden_num end)as punish_num,
$years as year,
$months as month,
$days as day
from
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders,
five_star_orders
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)='$start'
)driver
join(
select driver_id from service_security.complaint_info_com
where concat_ws('-',year,month,day)='$start'
)cic
on cic.driver_id=driver.driver_id
LEFT OUTER  JOIN
(
select driver_id,punish_num from service_security.driver_punish
where concat_ws('-',year,month,day)='$start'
)punish
on punish.driver_id=cic.driver_id
full outer join
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders,
five_star_orders,forbidden_num
from service_security.driver_order_info where concat_ws('-',year,month,day)='$start1'
)doi
on doi.driver_id=cic.driver_id
distribute by(year,month,day);"
======================================================================================



select
b.pas_id,
(case when a.passenger_complaint_orders is null
then cast(0 as bigint)
else a.passenger_complaint_orders end )+
(case when b.passenger_complaint_orders is null
then cast(0 as bigint)
else b.passenger_complaint_orders end )as

passenger_complaint_orders,

(case when a.driver_complaint_orders is null
then cast(0 as bigint)
else a.driver_complaint_orders end )+
(case when b.driver_complaint_orders is null
then cast(0 as bigint)
else b.driver_complaint_orders end )as

driver_complaint_orders,

(case when a.total_cancel_orders is null
then cast(0 as bigint)
else a.total_cancel_orders end )+
(case when b.total_cancel_orders is null
then cast(0 as bigint)
else b.total_cancel_orders end )as
total_cancel_orders

(select pas_id,
passenger_complaint_orders,
driver_complaint_orders,
(cancel_before_count+cancel_after_count) as total_cancel_orders
from
gulfstream_dw.dw_m_passenger_order
where concat_ws('-',year,month,day)=''
)a
left outer join
(
select pas_id,
passenger_complaint_orders,
driver_complaint_orders,
total_cancel_orders
from
service_security.passenger_order_info
where concat_ws('-',year,month,day)=''
)b
on
a.pas_id=b.pas_id;
select
select * from
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders, five_star_orders
from  dw_m_driver_order where concat_ws('-',year,month,day)='2017-05-19'
)driver
join
(
select driver_id from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
and concat_ws('-',year,month,day)='2017-05-19'
)pushlog
on pushlog.driver_id=driver.driver_id
limit 10;


select
pushlog.driver_id,
(case when driver.punish_num is null
then cast(0 as int)
else driver.punish_num end
)+pushlog.punishes
as punishes
from
(
select driver_id,punish_num
from  driver_punish where concat_ws('-',year,month,day)='2017-05-19'
)driver
left outer join
(
select driver_id,count(*) as punishes from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
and concat_ws('-',year,month,day)='2017-05-19' group by driver_id,year,month,day
)pushlog
on pushlog.driver_id=driver.driver_id





INSERT INTO TABLE SERVICE_SECURITY.driver_punish
PARTITION (year='2017',month='05',day='26')
select driver_id,
count(*) as punishes
from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
group by driver_id
distribute by (year,month,day);

create table if not exists service_security.driver_punish_temp
like  service_security.driver_punish
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_punish_temp';
----------------------------------------------
INSERT table service_security.driver_punish_temp
PARTITION(year,month,day)
select driver_id,
count(*) as punishes,
year,
month,
day
from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
where concat_ws('-',year,month,day)='' group by driver_id,year,month,day;

--------------------------------------------------------------------
INSERT table service_security.driver_punish
PARTITION(year,month,day)

select driver_id,punish_num from service_security.driver_punish_temp
union all
(
select driver_id,punish_num from driver_punish where concat_ws('-',year,month,day)=''
left outer join service_security.driver_punish_temp
on driver_punish.driver_id=driver_punish_temp.driver_id
)

select * from driver_punish where concat_ws('-',year,month,day)='2015-09-01'
full outer join
driver_punish_temp
on driver_punish.driver_id=driver_punish_temp.driver_id;

