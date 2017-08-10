#!/bin/bash
start=$1
years=date -d "$start" +%Y
months=date -d "$start" +%m
days=date -d "$start" +%d
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
source /home/common_plat_security/dlc/hiveset.hql;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;

INSERT INTO TABLE SERVICE_SECURITY.driver_punish
PARTITION (year,month,day)
select
a.driver_id,
a.punish_num,
'$years' as year,
'$months' as month,
'$days' as day
from
(
select driver_id,count(*)as punish_num from gulfstream_ods.g_driver_punish_log
where method not in(9,3)  and exec_status=1 and a_status=1 and
concat_ws('-',year,month,day)<='$start'  group by driver_id
)a
distribute by(year,month,day);"