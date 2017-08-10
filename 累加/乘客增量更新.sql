#!/bin/bash
start=$1
start1=`date -d "2 days $start" +%Y-%m-%d` 
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
a.pas_id,
b.passenger_complaint_orders,
b.driver_complaint_orders,
b.cancel_before_count,
b.cancel_after_count
from
(
select pas_id from gulfstream_dw.dw_v_passenger_base where concat_ws('-',year,month,day)='$start'
)a
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='$start'
)b
on a.pas_id=b.pas_id
UNION ALL
SELECT pas_id,passenger_complaint_orders,driver_complaint_orders,
cancel_before_count,cancel_after_count FROM  service_security.passenger_all_info_y
where concat_ws('-',year,month,day)='$start1'
)updata GROUP BY updata.pas_id
distribute by(year,month,day);"








SELECT 
updata.pas_id AS pas_id,
SUM(updata.passenger_complaint_orders) AS passenger_complaint_orders,
SUM(updata.driver_complaint_orders) AS driver_complaint_orders,
SUM(updata.cancel_before_count) AS cancel_before_count,
SUM(updata.cancel_after_count) AS cancel_after_count,

FROM
(

select
a.pas_id,
b.passenger_complaint_orders,
b.driver_complaint_orders,
b.cancel_before_count,
b.cancel_after_count
from
(
select passenger_id from major_com where concat_ws('-',year,month,day)='$start'
)a
join
(
select pas_id,passenger_complaint_orders,driver_complaint_orders,cancel_before_count,cancel_after_count
from gulfstream_dw.dw_m_passenger_order where concat_ws('-',year,month,day)='$start'
)b
on a.pas_id=b.pas_id
UNION ALL
SELECT pas_id,passenger_complaint_orders,driver_complaint_orders,
cancel_before_count,cancel_after_count FROM  service_security.passenger_order_info_y
where concat_ws('-',year,month,day)='$start1'
)updata GROUP BY updata.pas_id
distribute by(year,month,day);"