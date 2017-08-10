#!/bin/bash
start=$1
start1=`date -d  "2 days $start" +%Y%m%d `
years=`date -d "$start1" +%Y`
months=`date -d "$start1" +%m`
days=`date -d "$start1" +%d`

hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

insert into table service_security.driver_order_info_y
partition(year,month,day)
select
driver_id,
one_star_orders,
two_star_orders,
three_star_orders,
four_star_orders,
five_star_orders,
'$years' as year,
'$months' as month,
'$days' as day
from driver_order_info
where concat_ws('-',year,month,day)='$start'
distribute by(year,month,day);
"