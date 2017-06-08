countCREATE TABLE service_security.passenger_order_info(
pas_id              bigint      COMMENT'乘客ID',
passenger_complaint_orders string          COMMENT'快专车乘客投诉司机数',
driver_complaint_orders	 string             COMMENT '快专车司机投诉乘客数',
cancel_before_count bigint             COMMENT'抢单前乘客取消订单总数 ',
cancel_after_count  bigint COMMENT'抢单后乘客取消订单总数 '
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/passenger_order_info';

============================================================================

INSERT INTO TABLE service_security.passenger_order_info
PARTITION(year,month,day)
select
b.passenger_id,
a.passenger_complaint_orders,
a.driver_complaint_orders,
a.cancel_before_count,
a.cancel_after_count,
year,
month,
day
from
(
select passenger_id,year,month,day from service_security.complaint_info_com where concat_ws('-',year,month,day)='2015-09-01'
)b
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='2015-09-01'
)a
on
a.pas_id=b.passenger_id
distribute by(year,month,day);

================================================================================================

#!/bin/bash
start=$1
start1=`date -d "1 day ago $start" +%Y-%m-%d`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.passenger_order_info
PARTITION(year,month,day)
select
(case when b.passenger_id is null then c.pas_id else b.passenger_id end) as passenger_id,

(case when a.passenger_complaint_orders is null then cast(0 as bigint) else a.passenger_complaint_orders end )+
(case when c.passenger_complaint_orders is null then cast(0 as bigint) else c.passenger_complaint_orders end )as passenger_complaint_orders,

(case when a.driver_complaint_orders is null then cast(0 as bigint) else a.driver_complaint_orders end )+
(case when c.driver_complaint_orders is null then cast(0 as bigint) else c.driver_complaint_orders end )as driver_complaint_orders,

(case when a.cancel_before_count is null then cast(0 as bigint) else a.cancel_before_count end )+
(case when c.cancel_before_count is null then cast(0 as bigint) else c.cancel_before_count end )as cancel_before_count,

(case when a.cancel_after_count is null then cast(0 as bigint) else a.cancel_after_count end )+
(case when c.cancel_after_count is null then cast(0 as bigint) else c.cancel_after_count end )as cancel_after_count,
'$years',
'$months',
'$days'
from
(
select passenger_id,year,month,day
from service_security.complaint_info_com where concat_ws('-',year,month,day)='$start'
)b
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='$start'
)a
on
a.pas_id=b.passenger_id
full outer join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from service_security.passenger_order_info
where concat_ws('-',year,month,day)='$start1'
)c
on c.pas_id=b.passenger_id
distribute by(year,month,day);"
===============================================================
INSERT INTO TABLE service_security.passenger_order_info
PARTITION(year='2015',month='09',day='02')
select
(case when b.passenger_id is null then c.pas_id else b.passenger_id end) as passenger_id,

(case when a.passenger_complaint_orders is null then 0 else a.passenger_complaint_orders end )+
(case when c.passenger_complaint_orders is null then 0 else c.passenger_complaint_orders end )as passenger_complaint_orders,

(case when a.driver_complaint_orders is null then 0 else a.driver_complaint_orders end )+
(case when c.driver_complaint_orders is null then 0 else c.driver_complaint_orders end )as driver_complaint_orders,

(case when a.cancel_before_count is null then 0 else a.cancel_before_count end )+
(case when c.cancel_before_count is null then 0 else c.cancel_before_count end )as cancel_before_count,

(case when a.cancel_after_count is null then 0 else a.cancel_after_count end )+
(case when c.cancel_after_count is null then 0 else c.cancel_after_count end )as cancel_after_count,
'2015',
'09',
'02'
from
(
select passenger_id,year,month,day
from service_security.complaint_info_com where concat_ws('-',year,month,day)='2015-09-02'
)b
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='2015-09-02'
)a
on
a.pas_id=b.passenger_id
full outer join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from service_security.passenger_order_info
where concat_ws('-',year,month,day)='2015-09-01'
)c
on c.pas_id=b.passenger_id
distribute by(year,month,day);








重大投诉--complaint_info_com


 select product_id, concat_ws('_',collect_set(promotion_id)) as promotion_ids from product_promotion group by product_id;
 select order_id,concat_ws(';',collect_set(contents) as test from change_car_info group by orderid limit 1;
