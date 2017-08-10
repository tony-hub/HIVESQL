Create table service_security.driver_punish(
 driver_id BIGINT COMMENT'司机id',
 punish_num INT COMMENT '封禁次数'
 )
 PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
 LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_punish';

INSERT INTO TABLE SERVICE_SECURITY.driver_punish
PARTITION (year,month,day)
select driver_id,
count(*) as punishes,
year,
month,
day
from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
and concat_ws('-',year,month,day)='2015-11-18' group by driver_id,year,month,day
distribute by (year,month,day);
==========================2015-11-18
select
count(*) as punishes,concat_ws('-',year,month,day)
from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
and concat_ws('-',year,month,day)='2015-09-01' group by driver_id,year,month,day
=========================

date -d"1 day %start" +%Y-%m-%d

=============full outer join =============

#!/bin/bash
start=$1
start1=`date -d "1 day ago $start" +%Y-%m-%d`
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
source /home/common_plat_security/dlc/hiveset.hql;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;

INSERT INTO TABLE SERVICE_SECURITY.driver_punish
PARTITION (year,month,day)
select
(case when a.driver_id is null
then cast(b.driver_id as bigint)
else a.driver_id end ) as driver_id,
(case when a.punish_num is null
then cast(0 as bigint)
else a.punish_num end )+
(case when b.punish_num is null
then cast(0 as bigint)
else b.punish_num end )as
punish_num,
year,
month,
day
from
(
select driver_id,punish_num from driver_punish where concat_ws('-',year,month,day)='$start1'
)as a
FULL OUTER  JOIN
(
select driver_id,count(*) as punish_num,year,month,day
from gulfstream_ods.g_driver_punish_log where method!=9 and exec_status=1 and a_status=1
and concat_ws('-',year,month,day)='$start' group by driver_id,year,month,day
)as b
on a.driver_id=b.driver_id;
distribute by(year,month,day);


select *
from
(
select driver_id,punish_num from driver_punish where concat_ws('-',year,month,day)='2015-11-18'
)as a
JOIN
(
select driver_id,sum(count(*)) as punish_num,year,month,day from gulfstream_ods.g_driver_punish_log where method not in(9,3)  and exec_status=1 and a_status=1 and concat_ws('-',year,month,day)<='2015-11-18' group by driver_id,year,month,day
)as b
on a.driver_id=b.driver_id;


