#!/bin/bash

start=$1
start=`date -d "$start" +%Y-%m-%d`
start1=`date -d "1 days ago $start"`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

insert into table service_security.sexual_harassment_driver
partition(year,month,day)
select 
c.driver_id,
'$years' as year,
'$months' as month,
'$days' as day
from
(select distinct id,order_id,dict_id,complaint_result
from pdw.g_service_worksheet --电话投诉工单表
where concat_ws('-',year,month,day)='$start' and complaint_result=1 
and valid=1 and order_id!=0
)a
join
(   --查询有“投诉”字段的工单
select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='$start' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
on (a.dict_id=b.id)
join
(
select distinct order_id,driver_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='$start' and product_id in (3,4,7) 
and order_status in(5,7,11,12) 
)c
on (a.order_id=c.order_id)
union all
select * from service_security.sexual_harassment_driver where concat_ws('-',year,month,day)='$start1'
distribute by(year,month,day);"