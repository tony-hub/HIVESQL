#!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.driver_info_all
PARTITION(year,month,day)
select
driver.driver_id,
driver.one_star_orders,
driver.two_star_orders,
driver.three_star_orders,
driver.four_star_orders,
driver.five_star_orders,
'$years' as year,
'$months' as month,
'$days' as day
from
(
select driver_id from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='$start'
) driver_base
left outer join
(
select driver_id, sum(one_star_orders) as one_star_orders,sum(two_star_orders) as two_star_orders,
sum(three_star_orders) as three_star_orders,sum(four_star_orders) as four_star_orders,
sum(five_star_orders) as five_star_orders
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)<'$start'
group by driver_id
)driver
on driver_base.driver_id=driver.driver_id
distribute by(year,month,day);"
#!/bin/bash
start=$1
start1=`date -d "1 day ago $start" +%Y-%m-%d`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.passenger_order_info_normal
PARTITION(year,month,day)
select
b.pas_id,
a.passenger_complaint_orders,
a.driver_complaint_orders,
a.cancel_before_count,
a.cancel_after_count,
'$years'as year,
'$months' as month,
'$days' as day
from
(
select pas_id from gulfstream_dw.dw_v_passenger_base where concat_ws('-',year,month,day)='$start'
)b
left outer join
(
select pas_id,sum(passenger_complaint_orders) as passenger_complaint_orders,
sum(driver_complaint_orders) as driver_complaint_orders ,
sum(cancel_before_count) as cancel_before_count,
sum(cancel_after_count) as cancel_after_count from gulfstream_dw.dw_m_passenger_order
where concat_ws('-',year,month,day)<'$start' group by pas_id
)a
on a.pas_id=b.pas_id
distribute by(year,month,day);"