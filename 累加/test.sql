INSERT OVERWRITE TABLE service_security.passenger_order_info_y 
PARTITION(year,month,day) 
SELECT 
updata.pas_id AS pas_id,
SUM(updata.passenger_complaint_orders) AS passenger_complaint_orders,
SUM(updata.driver_complaint_orders) AS driver_complaint_orders,
SUM(updata.cancel_before_count) AS cancel_before_count,
SUM(updata.cancel_after_count) AS cancel_after_count
FROM 
(
select
b.pas_id,
b.passenger_complaint_orders,
b.driver_complaint_orders,
b.cancel_before_count,
b.cancel_after_count
from 
(
select passenger_id,year,month,day
from service_security.major_com where concat_ws('-',year,month,day)='2017-06-14' 
and passenger_id=5000227171637
)a
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='2017-06-14'
and pas_id=5000227171637
)b
on a.passenger_id=b.pas_id
UNION ALL
SELECT pas_id,passenger_complaint_orders,driver_complaint_orders,
cancel_before_count,cancel_after_count FROM  service_security.passenger_order_info_y
where concat_ws('-',year,month,day)='2017-06-14' and pas_id=5000227171637
)updata GROUP BY updata.pas_id
distribute by(year,month,day);




select driver_id,year,month,day from major_com where driver_id !=0 and type=0 and
path like '%性骚扰%'and path not like '%顺风车%'  and path not like '%交通事故%' and 
path not like '%失联%' and path not like '%延误行程%' and path not like '%出租车%' 
and product_id in (3,4,7) and order_status in(5,7,11,12) 
and order_id is not null and length(cast(order_id as string))>=6




#!/bin/bash
start=$1
start1=`date -d "1 day $start" +%Y-%m-%d` 
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

INSERT OVERWRITE TABLE service_security.passenger_all_info_y 
PARTITION(year,month,day) 
SELECT 
updata.pas_id AS pas_id,
SUM(updata.passenger_complaint_orders) AS passenger_complaint_orders,
SUM(updata.driver_complaint_orders) AS driver_complaint_orders,
SUM(updata.cancel_before_count) AS cancel_before_count,
SUM(updata.cancel_after_count) AS cancel_after_count,
'$years' as year,
'$months' as month,
'$days' as day
FROM
(
select
b.pas_id,
b.passenger_complaint_orders,
b.driver_complaint_orders,
b.cancel_before_count,
b.cancel_after_count
from
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='$start'
)b
UNION ALL
SELECT pas_id,passenger_complaint_orders,driver_complaint_orders,
cancel_before_count,cancel_after_count FROM  service_security.passenger_all_info_y
where concat_ws('-',year,month,day)='$start1'
)updata GROUP BY updata.pas_id
distribute by(year,month,day);





SELECT
updata.city_name,
updata.name,
updata.type
FROM
(
select
a.order_id,
a.name,
a.city_name
from
(
select order_id,starting_name as name,city_name from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='2017-07-01'
union 
select order_id,dest_name as name,city_name from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='2017-07-01'
)a
join
(
select b.order_id,b.type
from
(
select order_id,starting_poi_type as type  from service_security.poi_normal 
where concat_ws('-',year,month,day)='2017-07-01'
union
select order_id,dest_poi_type as type from service_security.poi_normal 
where concat_ws('-',year,month,day)='2017-07-01'
)b
)b1
on a.order_id=b1.order_id
UNION ALL
SELECT city_name,name,type FROM  service_security.poi_test
where concat_ws('-',year,month,day)='2016-07-01'
)updata GROUP BY updata.name,updata.type
distribute by(year,month,day);


INSERT OVERWRITE TABLE service_security.poi_test 
PARTITION(year,month,day) 
SELECT
a1.city_name,
a1.name,
b1.type,
'2016' as year,
'11' as month,
'01' as day
FROM
(
select b.order_id,b.type from
(
select order_id,starting_poi_type as type  from service_security.poi_normal 
where concat_ws('-',year,month,day)='2016-11-01'
union
select order_id,dest_poi_type as type from service_security.poi_normal 
where concat_ws('-',year,month,day)='2016-11-01'
)b 
)b1
join
(
select a.order_id, a.name,a.city_name from
(
select order_id,starting_name as name,city_name from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='2016-11-01' and starting_name is not null
union 
select order_id,dest_name as name ,city_name from gulfstream_dw.dw_v_order_base 
where concat_ws('-',year,month,day)='2016-11-01' and dest_name is not null
)a)a1
on a1.order_id=b1.order_id
GROUP BY a1.city_name,a1.name,b1.type
distribute by(year,month,day);