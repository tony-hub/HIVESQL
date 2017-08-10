#!/bin/bash
start=$1
time1=$2
time2=$3
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.dis_time
PARTITION(year,month,day)
select
order_id,
driver_id,
passenger_id,
round(normal_distance/cast(start_dest_distance/1000.0 as double),4)as distance_rate,
(case when finish_time ='0000-00-00 00:00:00'  then 0  else
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/c.forecast_time,4)
end) as time_rate
'$years' as year,
'$months' as month,
'$days' as day
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)='$start' and begin_charge_time !='0000-00-00 00:00:00'
and start_dest_distance!=0 and normal_distance>5 and
 (a_birth_time  between concat_ws(' ','$start','00:00:00') and concat_ws(' ','$start','$time1')or
 a_birth_time between concat_ws(' ','$start','$time2') and concat_ws(' ','$start','23:59:59'))
distribute by (year,month,day);"


select * from driver_applist where installed_app like '%快播%';
select driver_id,length(id) from driver_applist  where  installed_app like '%快播%' limit 1000;
"
INSERT INTO TABLE service_security.dis_time_t
PARTITION(year,month,day)
select * from dis_time

order  by time_rate desc;
select count(*),concat_ws('-',year,month,day)
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)>='2017-05-20' and concat_ws('-',year,month,day)<='2017-05-24'
group by year,month,day
