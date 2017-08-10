#!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"

set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

insert into table service_security.driver_applist_all
PARTITION(year,month,day)
select
a.driver_id,
b.installed_apps,
b.brand,
'$years' as year,
'$months' as month,
'$days' as day
from
(
select distinct driver_id from gulfstream_ods.g_driver where concat_ws('-',year,month,day)='$start'
and driver_id !=0
)a
join
(
select uid ,regexp_extract(attrs['installed_apps'],'[^,]+',0) as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='$start' and appid in(5,6) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and uid !=0
)b
on a.driver_id=b.uid
distribute by(year,month,day);"


select
a.driver_id,
b.installed_apps,
b.brand
from
(
select distinct driver_id from gulfstream_ods.g_driver where concat_ws('-',year,month,day)='2017-05-20'
and driver_id !=0
)a
join
(
select uid ,regexp_extract(attrs['installed_apps'],'[^,]+',0) as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-20' and appid in(5,6) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and uid !=0
)b
on a.driver_id=b.uid limit 100
distribute by(year,month,day);