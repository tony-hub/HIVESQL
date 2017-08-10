create table service_security.driver_xsr_num
(
driver_id bigint COMMENT'司机ID',
xsr_num int COMMENT'被投诉性骚扰次数'
)
partitioned by(
`year` string,
`month` string,
`day` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/driver_xsr_num'


#!/bin/bash

start=$1
start=`date -d "$start" +%Y-%m-%d`
start1=`date -d "2 days ago $start"`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`
hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

insert overwrite table service_security.driver_xsr_num
select b.driver_id,
case when a.cnt is null then 0
else a.cnt as num,
'$year' as year,
'$month' as month,
'$day' as day
(
select driver_id from major_com
where concat_ws('-',year,month,day)<='$start'
)b
left outer join
(select driver_id,count(*) as cnt from  major_com_y
where concat_ws('-',year,month,day)<='$start1' and path like '%性骚扰%'
group by driver_id
)a
on a.driver_id=b.driver_id;
"



select b.driver_id,
case when a.driver_id is null then 0
else a.cnt   end as num
from
(
select driver_id from major_com
where concat_ws('-',year,month,day)<='2015-09-01' and driver_id !=0
)b
left outer join
(select driver_id,count(*) as cnt from  major_com
where concat_ws('-',year,month,day)<='2015-08-30' and path like '%性骚扰%'
group by driver_id
)a
on a.driver_id=b.driver_id;